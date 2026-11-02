"""Tableau de bord RetailPulse (Streamlit).

Lit uniquement les données persistées dans PostgreSQL : les agrégats métier
(sales_aggregates) sont calculés par Spark et ne sont PAS recalculés ici ; le
dashboard se limite à des totaux d'affichage et à des tris.
Actualisation automatique toutes les REFRESH_SECONDS secondes (10 par défaut).
"""
import os
from datetime import datetime, timezone

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

QUERIES = {
    "kpis": """
        SELECT
            (SELECT coalesce(sum(total_revenue), 0) FROM sales_aggregates)   AS revenue,
            (SELECT coalesce(sum(total_quantity), 0) FROM sales_aggregates)  AS items_sold,
            (SELECT count(DISTINCT category) FROM sales_aggregates)          AS categories,
            (SELECT count(*) FROM orders)                                    AS orders,
            (SELECT count(*) FROM orders WHERE status = 'PAID')              AS paid_orders,
            (SELECT count(*) FROM orders WHERE status IN ('PAYMENT_FAILED', 'CANCELLED'))
                                                                             AS failed_orders,
            (SELECT max(updated_at) FROM sales_aggregates)                   AS last_aggregate,
            (SELECT max(ingested_at) FROM order_events)                      AS last_event
    """,
    "timeline": """
        SELECT window_start, sum(total_revenue) AS revenue, sum(order_count) AS order_lines
        FROM sales_aggregates
        WHERE window_start >= now() - interval '3 hours'
        GROUP BY window_start ORDER BY window_start
    """,
    "by_category": """
        SELECT category, sum(total_revenue) AS revenue, sum(total_quantity) AS quantity,
               sum(order_count) AS orders
        FROM sales_aggregates GROUP BY category ORDER BY revenue DESC
    """,
    "latest_orders": """
        SELECT o.order_id, o.created_at, o.status, o.total_amount,
               c.first_name || ' ' || c.last_name AS customer, c.city
        FROM orders o JOIN customers c ON c.customer_id = o.customer_id
        ORDER BY o.created_at DESC LIMIT 15
    """,
    "latest_aggregates": """
        SELECT category, window_start, window_end, order_count, total_quantity,
               total_revenue, updated_at
        FROM sales_aggregates ORDER BY updated_at DESC LIMIT 15
    """,
}

STATUS_LABELS = {"PAID": "✅ Payée", "CREATED": "🕒 Créée",
                 "PAYMENT_FAILED": "❌ Paiement échoué", "CANCELLED": "🚫 Annulée"}


def money(value):
    return f"{float(value):,.2f} {CURRENCY}".replace(",", " ")


def load():
    with psycopg.connect(CONNINFO, connect_timeout=3) as conn:
        frames = {}
        for name, sql in QUERIES.items():
            with conn.cursor() as cur:
                cur.execute(sql)
                columns = [d.name for d in cur.description]
                frames[name] = pd.DataFrame(cur.fetchall(), columns=columns)
    return frames


def freshness(kpis):
    last = kpis["last_aggregate"]
    if last is None or pd.isna(last):
        return "⚪ Aucune donnée agrégée pour l'instant", None
    age = (datetime.now(timezone.utc) - last).total_seconds()
    if age > STALE_AFTER_S:
        return f"🟠 Données figées depuis {int(age)} s (pipeline arrêté ?)", age
    return f"🟢 Données à jour (dernier agrégat il y a {int(age)} s)", age


st.set_page_config(page_title="RetailPulse", page_icon="📈", layout="wide")
st.title("RetailPulse — activité e-commerce en quasi temps réel")
st.caption("Source : PostgreSQL (agrégats Spark Structured Streaming). "
           f"Chiffre d'affaires = paiements réussis uniquement, en {CURRENCY}, prix remisés. "
           f"Actualisation automatique toutes les {REFRESH_SECONDS} s.")


@st.fragment(run_every=REFRESH_SECONDS)
def render():
    refreshed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        data = load()
    except psycopg.Error as exc:
        st.error(f"Connexion à PostgreSQL impossible : {exc}")
        st.caption(f"Dernière tentative : {refreshed_at}")
        return

    kpis = data["kpis"].iloc[0]
    status, _ = freshness(kpis)
    left, right = st.columns([3, 2])
    left.markdown(f"**État des données :** {status}")
    right.markdown(f"**Dernière actualisation :** {refreshed_at}")

    cols = st.columns(6)
    cols[0].metric(f"Chiffre d'affaires ({CURRENCY})", money(kpis["revenue"]))
    cols[1].metric("Commandes", f"{int(kpis['orders'])}")
    cols[2].metric("Commandes payées", f"{int(kpis['paid_orders'])}")
    cols[3].metric("Échouées / annulées", f"{int(kpis['failed_orders'])}")
    cols[4].metric("Articles vendus", f"{int(kpis['items_sold'])}")
    cols[5].metric("Catégories", f"{int(kpis['categories'])}")

    if data["by_category"].empty:
        st.info("Aucun paiement réussi agrégé pour l'instant : lancer le producteur et Spark.")
        return

    timeline = data["timeline"].astype({"revenue": float})
    st.subheader("Évolution du chiffre d'affaires (fenêtres de 5 minutes, 3 dernières heures)")
    st.altair_chart(
        alt.Chart(timeline).mark_bar().encode(
            x=alt.X("window_start:T", title="Début de fenêtre (UTC)"),
            y=alt.Y("revenue:Q", title=f"CA ({CURRENCY})"),
            tooltip=[alt.Tooltip("window_start:T", title="Fenêtre", format="%H:%M"),
                     alt.Tooltip("revenue:Q", title=f"CA ({CURRENCY})", format=",.2f")],
        ).properties(height=260),
        use_container_width=True,
    )

    by_category = data["by_category"].astype({"revenue": float, "quantity": int, "orders": int})
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Répartition du CA par catégorie")
        st.altair_chart(
            alt.Chart(by_category).mark_arc(innerRadius=60).encode(
                theta=alt.Theta("revenue:Q"),
                color=alt.Color("category:N", title="Catégorie", sort="-x"),
                tooltip=["category", alt.Tooltip("revenue:Q", format=",.2f", title=f"CA ({CURRENCY})")],
            ).properties(height=320),
            use_container_width=True,
        )
    with c2:
        st.subheader("Top 10 des catégories")
        st.altair_chart(
            alt.Chart(by_category.head(10)).mark_bar().encode(
                x=alt.X("revenue:Q", title=f"CA ({CURRENCY})"),
                y=alt.Y("category:N", sort="-x", title=None),
                tooltip=["category", alt.Tooltip("revenue:Q", format=",.2f"), "quantity", "orders"],
            ).properties(height=320),
            use_container_width=True,
        )

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Dernières commandes")
        orders = data["latest_orders"].copy()
        orders["status"] = orders["status"].map(STATUS_LABELS).fillna(orders["status"])
        st.dataframe(orders.rename(columns={
            "order_id": "Commande", "created_at": "Créée le (UTC)", "status": "Statut",
            "total_amount": f"Montant ({CURRENCY})", "customer": "Client", "city": "Ville"}),
            hide_index=True, use_container_width=True)
    with c4:
        st.subheader("Dernières agrégations mises à jour")
        st.dataframe(data["latest_aggregates"].rename(columns={
            "category": "Catégorie", "window_start": "Début", "window_end": "Fin",
            "order_count": "Commandes", "total_quantity": "Quantité",
            "total_revenue": f"CA ({CURRENCY})", "updated_at": "Mis à jour (UTC)"}),
            hide_index=True, use_container_width=True)


render()
