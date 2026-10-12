#!/bin/bash
# Lá»‡nh ngáº¯n cho bÃ i lab (cháº¡y trÃªn mÃ¡y cá»§a báº¡n, thá»±c thi trong container Airflow Ä‘ang cháº¡y).
#   scripts/lab.sh step <1..5|all> [--reset]   cháº¡y Tá»ªNG cáº¥u pháº§n, in káº¿t quáº£ vÃ  kiá»ƒm tra (step all = cáº£ pipeline)
#   scripts/lab.sh exp <idempotency|break-gate|leakage|threshold>   thÃ­ nghiá»‡m, ghi báº±ng chá»©ng vÃ o report/evidence/
#   scripts/lab.sh verify [--module N]     kiá»ƒm tra tá»± Ä‘á»™ng (27 kiá»ƒm tra, 5 module); --module 2 chá»‰ cháº¡y module 2
#   scripts/lab.sh test                    unit test (dhash, blur_score...)
#   scripts/lab.sh run [--reset]           cháº¡y cáº£ pipeline khÃ´ng qua Airflow (â‰ˆ 1 phÃºt)
#   scripts/lab.sh cli <lá»‡nh> [tham sá»‘]    vÃ­ dá»¥: cli ingest --drive drive_a | cli history silver.frames | cli sql "select 1"
#   scripts/lab.sh dbt <lá»‡nh> [tham sá»‘]    vÃ­ dá»¥: dbt run --select silver_telemetry | dbt test --select gold_dataset_frames
#   scripts/lab.sh shell                   má»Ÿ shell trong container
set -e
cd "$(dirname "$0")/.."
PY=/home/airflow/lab-venv/bin/python
X="docker compose exec -T"
case "${1:-}" in
  step)   shift; $X -w /opt/lab airflow $PY -m pipeline.cli step "$@" ;;
  exp)    shift; $X -w /opt/lab airflow $PY -m pipeline.cli exp "$@" ;;
  verify) shift; $X -w /opt/lab airflow $PY -m pipeline.cli verify "$@" ;;
  test)   $X -w /opt/lab airflow $PY -m unittest discover -s tests ;;
  run)    shift; $X -w /opt/lab airflow scripts/run_pipeline.sh "$@" ;;
  cli)    shift; $X -w /opt/lab airflow $PY -m pipeline.cli "$@" ;;
  dbt)    shift; $X -w /opt/dbt airflow /home/airflow/lab-venv/bin/dbt "$@" --no-use-colors ;;
  shell)  docker compose exec airflow bash ;;
  *) sed -n 2,11p "$0"; exit 1 ;;
esac
