from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="copy_live_casino_bets_to_analytics",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "ingestion", "live_casino"],
) as dag:

    copy_live_casino_bets = SQLExecuteQueryOperator(
        task_id="copy_live_casino_bets",
        conn_id="analytics_db",
        sql="""
            INSERT INTO public.dag_test_table (
                bet_id,
                user_id,
                game_id,
                provider_id,
                table_id,
                round_id,
                bet_type,
                stake_amount,
                win_amount,
                currency,
                bet_status,
                device_type,
                country_code,
                dealer_id,
                event_time,
                inserted_at
            )
            SELECT
                s.bet_id,
                s.user_id,
                s.game_id,
                s.provider_id,
                s.table_id,
                s.round_id,
                s.bet_type,
                s.stake_amount,
                s.win_amount,
                s.currency,
                s.bet_status,
                s.device_type,
                s.country_code,
                s.dealer_id,
                s.event_time,
                s.inserted_at
            FROM public.raw_live_casino_bets s
            WHERE NOT EXISTS (
                SELECT 1
                FROM dag_test_table t
                WHERE t.bet_id = s.bet_id
            );
        """,
    )