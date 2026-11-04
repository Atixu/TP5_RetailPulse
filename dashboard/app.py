"""Tableau de bord RetailPulse (Streamlit).

Lit uniquement les données persistées dans PostgreSQL. Les agrégats métier
(sales_aggregates) sont calculés par Spark et ne sont PAS recalculés ici : le
dashboard fait des sommes d'affichage sur une période, des comptages sur le
détail des commandes et des ratios entre ces valeurs (définitions en bas de page).
Actualisation automatique toutes les REFRESH_SECONDS secondes (10 par défaut).
"""
import os
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "10"))
CURRENCY = os.environ.get("CURRENCY", "USD")
STALE_AFTER_S = int(os.environ.get("STALE_AFTER_SECONDS", "120"))
CONNINFO = (
    f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'retailpulse')} "
    f"user={os.environ.get('POSTGRES_USER', 'retailpulse')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', '')}"
)

PERIODS = {"1 h": timedelta(hours=1), "6 h": timedelta(hours=6),
           "24 h": timedelta(hours=24), "Tout": None}

# Palette de référence (skill dataviz) : une seule teinte pour les séries,
# encre et traits discrets, couleurs de statut réservées aux états.
SERIES = "#2a78d6"
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
STATUS_GOOD, STATUS_WARNING, STATUS_CRITICAL = "#0ca30c", "#fab219", "#d03b3b"

STATUS_LABELS = {"PAID": "Payée", "CREATED": "En attente de paiement",
                 "PAYMENT_FAILED": "Paiement refusé", "CANCELLED": "Annulée"}

# Le filtre « (since IS NULL OR col >= since) » applique la période choisie.
QUERIES = {
    "sales": """
        SELECT coalesce(sum(total_revenue), 0)  AS revenue,
               coalesce(sum(total_quantity), 0) AS items,
               count(DISTINCT category)          AS categories
        FROM sales_aggregates
        WHERE (%(since)s::timestamptz IS NULL OR window_start >= %(since)s)
    """,
    "sales_prev": """
        SELECT coalesce(sum(total_revenue), 0) AS revenue
        FROM sales_aggregates
        WHERE window_start >= %(prev)s AND window_start < %(since)s
    """,
    "orders": """
        SELECT count(*)                                              AS total,
               count(*) FILTER (WHERE status = 'PAID')                AS paid,
               count(*) FILTER (WHERE status = 'PAYMENT_FAILED')      AS failed,
               count(*) FILTER (WHERE status = 'CANCELLED')           AS cancelled,
               count(DISTINCT customer_id) FILTER (WHERE status = 'PAID') AS customers
        FROM orders
        WHERE (%(since)s::timestamptz IS NULL OR created_at >= %(since)s)
    """,
    "orders_prev": """
        SELECT count(*) FILTER (WHERE status = 'PAID') AS paid
        FROM orders WHERE created_at >= %(prev)s AND created_at < %(since)s
    """,
    "pipeline": """
        SELECT
          (SELECT count(*) FROM order_events
             WHERE ingested_at >= now() - interval '5 minutes')               AS events_5m,
          (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_timestamp))
             FROM order_events WHERE ingested_at >= now() - interval '5 minutes') AS latency_p50,
          (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_timestamp))
             FROM order_events WHERE ingested_at >= now() - interval '5 minutes') AS latency_p95,
          (SELECT count(*) FROM invalid_events
             WHERE %(since)s::timestamptz IS NULL OR detected_at >= %(since)s)  AS rejected,
          (SELECT max(updated_at) FROM sales_aggregates)                       AS last_aggregate
    """,
    "timeline": """
        SELECT window_start, window_end, sum(total_revenue) AS revenue, sum(total_quantity) AS items
        FROM sales_aggregates
        WHERE window_start >= coalesce(%(since)s::timestamptz, now() - interval '24 hours')
        GROUP BY window_start, window_end ORDER BY window_start
    """,
    "by_category": """
        SELECT category, sum(total_revenue) AS revenue, sum(total_quantity) AS items
        FROM sales_aggregates
        WHERE (%(since)s::timestamptz IS NULL OR window_start >= %(since)s)
        GROUP BY category ORDER BY revenue DESC
    """,
    "latest_orders": """
        SELECT o.order_id, o.created_at, o.status, o.total_amount,
               c.first_name || ' ' || c.last_name AS customer, c.city
        FROM orders o JOIN customers c ON c.customer_id = o.customer_id
        ORDER BY o.created_at DESC LIMIT 20
    """,
    "latest_aggregates": """
        SELECT category, window_start, window_end, order_count, total_quantity,
               total_revenue, updated_at
        FROM sales_aggregates ORDER BY updated_at DESC LIMIT 20
    """,
}


def load(since, prev):
    params = {"since": since, "prev": prev}
    frames = {}
    with psycopg.connect(CONNINFO, connect_timeout=3) as conn:
        for name, sql in QUERIES.items():
            if name.endswith("_prev") and since is None:
                continue
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [d.name for d in cur.description]
                frames[name] = pd.DataFrame(cur.fetchall(), columns=columns)
    return frames


# --- Formatage ---------------------------------------------------------------------

def number(value, decimals=0):
    text = f"{float(value):,.{decimals}f}".replace(",", " ").replace(".", ",")
    return text


def money(value):
    return f"{number(value, 2)} {CURRENCY}"


def percent(value):
    return "—" if value is None else f"{number(value * 100, 1)} %"


def delta(current, previous):
    if previous is None or float(previous) == 0:
        return None
    change = (float(current) - float(previous)) / float(previous) * 100
    return f"{'+' if change >= 0 else ''}{number(change, 1)} % vs période précédente"


def ratio(numerator, denominator):
    return float(numerator) / float(denominator) if denominator else None


# --- Graphiques --------------------------------------------------------------------

def base_chart(data):
    return alt.Chart(data)


def configure(chart):
    # Fond et marge au niveau du graphique final : Altair les refuse sur les
    # sous-graphiques d'une superposition (barres + étiquettes).
    return (chart
            .properties(background=SURFACE, padding=12)
            .configure_view(strokeWidth=0)
            .configure_axis(labelColor=MUTED, titleColor=INK_2, gridColor=GRID,
                            domainColor=AXIS, tickColor=AXIS, labelFontSize=12,
                            titleFontSize=12, titleFontWeight="normal")
            .configure_axisX(grid=False))


def timeline_chart(timeline):
    # Chaque barre couvre sa fenêtre [début, fin[, avec 2 px d'espace entre deux fenêtres.
    return configure(base_chart(timeline).mark_bar(
        color=SERIES, cornerRadiusTopLeft=4, cornerRadiusTopRight=4, x2Offset=-2,
    ).encode(
        x=alt.X("window_start:T", title="Heure (UTC)", scale=alt.Scale(type="utc"),
                axis=alt.Axis(format="%H:%M", labelAngle=0, tickCount=8)),
        x2="window_end:T",
        y=alt.Y("revenue:Q", title=f"Chiffre d'affaires ({CURRENCY})",
                axis=alt.Axis(format="~s", tickCount=4)),
        y2=alt.datum(0),  # barres ancrées sur la ligne de base
        tooltip=[alt.Tooltip("utcyearmonthdatehoursminutes(window_start):T",
                             title="Fenêtre (UTC)", format="%d/%m %H:%M"),
                 alt.Tooltip("revenue:Q", title=f"CA ({CURRENCY})", format=",.2f"),
                 alt.Tooltip("items:Q", title="Articles vendus", format=",")],
    ).properties(height=280))


def category_chart(by_category, top=10):
    data = by_category.head(top).assign(label=lambda d: d["revenue"].map(lambda v: number(v)))
    bars = base_chart(data).mark_bar(
        color=SERIES, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, height={"band": 0.7},
    ).encode(
        x=alt.X("revenue:Q", title=None, axis=alt.Axis(format="~s", tickCount=4)),
        y=alt.Y("category:N", sort="-x", title=None,
                axis=alt.Axis(labelLimit=180, labelColor=INK_2, domain=False, ticks=False)),
        tooltip=[alt.Tooltip("category:N", title="Catégorie"),
                 alt.Tooltip("revenue:Q", title=f"CA ({CURRENCY})", format=",.2f"),
                 alt.Tooltip("items:Q", title="Articles vendus", format=",")],
    )
    # Libellé de valeur uniquement en bout de barre (lecture sans survol).
    labels = bars.mark_text(align="left", dx=6, color=INK_2, fontSize=12).encode(
        text="label:N")
    return configure((bars + labels).properties(height=max(160, 30 * len(data))))


# --- Mise en page ------------------------------------------------------------------

st.set_page_config(page_title="RetailPulse", layout="wide")
st.markdown(
    """
    <style>
      .block-container {padding-top: 2rem; max-width: 1400px;}
      [data-testid="stMetricValue"] {font-size: 1.75rem; font-weight: 600;}
      [data-testid="stMetricLabel"] p {color: #52514e; font-size: 0.85rem;}
      [data-testid="stMetricDelta"] {font-size: 0.8rem;}
      h3 {font-weight: 600; font-size: 1.05rem; margin-top: 0.5rem;}
      .rp-sub {color: #52514e; margin-top: -0.75rem;}
      .rp-status {display: inline-flex; align-items: center; gap: .5rem; color: #0b0b0b;
                  font-size: .9rem; padding: .35rem .75rem; border-radius: 999px;
                  border: 1px solid rgba(11,11,11,.10); background: #fcfcfb;}
      .rp-dot {width: .6rem; height: .6rem; border-radius: 50%; display: inline-block;}
      .rp-note {color: #898781; font-size: .8rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

head_left, head_right = st.columns([3, 2], vertical_alignment="bottom")
head_left.title("RetailPulse")
head_left.markdown('<p class="rp-sub">Suivi des ventes e-commerce en quasi temps réel</p>',
                   unsafe_allow_html=True)
with head_right:
    period_label = st.radio("Période", list(PERIODS), index=1, horizontal=True,
                            label_visibility="collapsed")
status_slot = head_right.empty()


def status_badge(last_aggregate, now):
    if last_aggregate is None or pd.isna(last_aggregate):
        color, text = MUTED, "Aucune donnée agrégée"
    else:
        age = int((now - last_aggregate).total_seconds())
        if age > STALE_AFTER_S:
            color, text = STATUS_WARNING, f"Données figées depuis {age} s"
        else:
            color, text = STATUS_GOOD, f"Données à jour · agrégat il y a {age} s"
    return (f'<div style="text-align:right"><span class="rp-status">'
            f'<span class="rp-dot" style="background:{color}"></span>{text}'
            f' · actualisé à {now:%H:%M:%S} UTC</span></div>')


@st.fragment(run_every=REFRESH_SECONDS)
def render(period_label):
    now = datetime.now(timezone.utc)
    span = PERIODS[period_label]
    since = now - span if span else None
    prev = now - 2 * span if span else None
    try:
        data = load(since, prev)
    except psycopg.Error as exc:
        status_slot.markdown(
            f'<div style="text-align:right"><span class="rp-status"><span class="rp-dot" '
            f'style="background:{STATUS_CRITICAL}"></span>Base de données injoignable · '
            f'{now:%H:%M:%S} UTC</span></div>', unsafe_allow_html=True)
        st.error(f"Connexion à PostgreSQL impossible : {exc}")
        return

    sales, orders, pipe = data["sales"].iloc[0], data["orders"].iloc[0], data["pipeline"].iloc[0]
    status_slot.markdown(status_badge(pipe["last_aggregate"], now), unsafe_allow_html=True)
    prev_revenue = data["sales_prev"].iloc[0]["revenue"] if span else None
    prev_paid = data["orders_prev"].iloc[0]["paid"] if span else None
    settled = int(orders["paid"]) + int(orders["failed"]) + int(orders["cancelled"])

    # Activité commerciale
    st.subheader("Activité commerciale" + ("" if span is None else f" · dernières {period_label}"))
    row = st.columns(6)
    refused = int(orders["failed"]) + int(orders["cancelled"])
    tiles = [
        (f"Chiffre d'affaires ({CURRENCY})", number(sales["revenue"], 2),
         delta(sales["revenue"], prev_revenue), None),
        ("Commandes payées", number(orders["paid"]), delta(orders["paid"], prev_paid),
         f"sur {number(orders['total'])} commandes créées"),
        (f"Panier moyen ({CURRENCY})",
         "—" if not orders["paid"] else number(float(sales["revenue"]) / int(orders["paid"]), 2),
         None, None),
        ("Taux de paiement réussi", percent(ratio(orders["paid"], settled)), None,
         f"{number(refused)} refusées ou annulées"),
        ("Articles vendus", number(sales["items"]), None,
         f"dans {number(sales['categories'])} catégories"),
        ("Clients acheteurs", number(orders["customers"]), None, None),
    ]
    for column, (label, value, change, note) in zip(row, tiles):
        with column.container(border=True):
            st.metric(label, value, change)
            if note:
                st.caption(note)

    # Graphiques
    if data["by_category"].empty:
        st.info("Aucun paiement réussi sur cette période. Le producteur et Spark sont-ils démarrés ?")
    else:
        left, right = st.columns([3, 2], gap="large")
        with left:
            st.subheader("Chiffre d'affaires par fenêtre de 5 minutes")
            st.altair_chart(timeline_chart(data["timeline"].astype({"revenue": float, "items": int})),
                            use_container_width=True)
            if span is None:
                st.markdown('<p class="rp-note">Graphique limité aux 24 dernières heures.</p>',
                            unsafe_allow_html=True)
        with right:
            by_category = data["by_category"].astype({"revenue": float, "items": int})
            st.subheader(f"Top 10 des catégories sur {len(by_category)}")
            st.altair_chart(category_chart(by_category), use_container_width=True)

    # Santé du pipeline
    st.subheader("Santé du pipeline · 5 dernières minutes")
    row = st.columns(4)
    p50, p95 = pipe["latency_p50"], pipe["latency_p95"]
    last = pipe["last_aggregate"]
    age = None if last is None or pd.isna(last) else int((now - last).total_seconds())
    tiles = [
        ("Débit traité", f"{number(int(pipe['events_5m']) / 5, 1)} évt/min",
         f"{number(pipe['events_5m'])} événements enregistrés"),
        ("Latence médiane", "—" if p50 is None or pd.isna(p50) else f"{number(p50, 1)} s",
         None if p95 is None or pd.isna(p95) else f"émission → base · p95 {number(p95, 1)} s"),
        ("Dernier agrégat écrit", "—" if age is None else f"il y a {age} s",
         f"micro-batch Spark toutes les 5 s"),
        ("Messages rejetés", number(pipe["rejected"]),
         "invalides ou impossibles à enrichir" + ("" if span is None else ", sur la période")),
    ]
    for column, (label, value, note) in zip(row, tiles):
        with column.container(border=True):
            st.metric(label, value)
            st.caption(note)

    # Détail
    st.subheader("Détail")
    tab_orders, tab_aggregates, tab_categories = st.tabs(
        ["Dernières commandes", "Dernières agrégations", "Toutes les catégories"])
    with tab_orders:
        orders_df = data["latest_orders"].copy()
        orders_df["status"] = orders_df["status"].map(STATUS_LABELS).fillna(orders_df["status"])
        st.dataframe(orders_df, hide_index=True, use_container_width=True, column_config={
            "order_id": "Commande",
            "created_at": st.column_config.DatetimeColumn("Créée le (UTC)", format="DD/MM HH:mm:ss"),
            "status": "Statut",
            "total_amount": st.column_config.NumberColumn(f"Montant ({CURRENCY})", format="%.2f"),
            "customer": "Client",
            "city": "Ville",
        })
    with tab_aggregates:
        st.dataframe(data["latest_aggregates"], hide_index=True, use_container_width=True,
                     column_config={
                         "category": "Catégorie",
                         "window_start": st.column_config.DatetimeColumn("Début", format="DD/MM HH:mm"),
                         "window_end": st.column_config.DatetimeColumn("Fin", format="HH:mm"),
                         "order_count": "Commandes",
                         "total_quantity": "Articles",
                         "total_revenue": st.column_config.NumberColumn(f"CA ({CURRENCY})", format="%.2f"),
                         "updated_at": st.column_config.DatetimeColumn("Mis à jour (UTC)",
                                                                        format="HH:mm:ss"),
                     })
    with tab_categories:
        st.dataframe(data["by_category"], hide_index=True, use_container_width=True, column_config={
            "category": "Catégorie",
            "revenue": st.column_config.NumberColumn(f"CA ({CURRENCY})", format="%.2f"),
            "items": "Articles vendus",
        })


render(period_label)

with st.expander("Définitions des indicateurs"):
    st.markdown(f"""
- **Chiffre d'affaires** : somme des agrégats Spark (`sales_aggregates`) dont la fenêtre commence
  dans la période ; seuls les paiements réussis y sont comptés, en {CURRENCY}, prix remisés.
- **Commandes payées**, **refusées ou annulées**, **clients acheteurs** : comptage des commandes
  (`orders`) créées dans la période, selon leur statut.
- **Panier moyen** : chiffre d'affaires ÷ commandes payées.
- **Taux de paiement réussi** : commandes payées ÷ commandes dont le paiement a abouti ou échoué.
- **Variation** : par rapport à la période de même durée qui précède.
- **Débit** et **latence** : événements enregistrés dans `order_events` au cours des 5 dernières
  minutes ; latence = heure d'enregistrement − horodatage de l'événement.
- **Messages rejetés** : messages isolés par Spark dans `invalid_events` (JSON invalide, champ
  manquant, produit inconnu…).

Actualisation automatique toutes les {REFRESH_SECONDS} s. Le tableau de bord ne recalcule aucun
agrégat métier : il additionne et compare des valeurs déjà calculées et stockées.
""")
