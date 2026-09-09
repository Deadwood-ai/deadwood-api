#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

if [[ ! -f .env ]]; then
	cp .env.example .env
fi

if [[ -z "${COMPOSE_PROJECT_NAME:-}" ]]; then
	compose_project_name="$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env | tail -n 1)"
	compose_project_name="${compose_project_name%\"}"
	compose_project_name="${compose_project_name#\"}"
	compose_project_name="${compose_project_name%\'}"
	compose_project_name="${compose_project_name#\'}"
	export COMPOSE_PROJECT_NAME="${compose_project_name:-deadtrees-test}"
else
	export COMPOSE_PROJECT_NAME
fi

mkdir -p \
	data/archive \
	data/cogs \
	data/thumbnails \
	data/label_objects \
	data/downloads \
	data/raw_images \
	data/trash

if command -v deadtrees >/dev/null 2>&1; then
	DEADTREES_CLI=(deadtrees)
elif [[ -x venv/bin/deadtrees ]]; then
	DEADTREES_CLI=(venv/bin/deadtrees)
else
	echo "Could not find the deadtrees CLI. Install it or create the repo venv first." >&2
	exit 1
fi

"${DEADTREES_CLI[@]}" dev test api api/tests/test_settings.py

api_smoke_tests=(
	api/tests/routers/test_contributor_contract_smoke.py
	api/tests/routers/test_upload_retries.py
	api/tests/upload/test_chunk_session.py
	api/tests/routers/test_upload_odm_detection.py
	api/tests/routers/test_process.py
	api/tests/routers/test_prepackaged.py
	api/tests/routers/test_dte_stats.py
	api/tests/routers/test_download.py::test_download_status_invalid_dataset_id_returns_400
	api/tests/routers/test_download.py::TestMultiBundleHelpers
	api/tests/db/test_auditor_flag_review_contract.py
	api/tests/db/test_operator_monitoring_access.py
	api/tests/db/test_dataset_rls_policy.py
	api/tests/db/test_privileged_users.py
	api/tests/db/test_dataset_audit.py
	api/tests/db/test_dataset_edit_history.py
	api/tests/db/test_data_publication.py
	api/tests/db/test_process_priority.py
	api/tests/db/test_processor_prediction_rls.py
	api/tests/db/test_odm_database.py
	api/tests/db/test_priwa_field_schema.py
	api/tests/db/test_processing_notification_preferences.py
	api/tests/test_notifications.py
	api/tests/test_search_embed.py
	api/tests/test_export_reference_patches.py
	shared/tests
)

docker compose -f docker-compose.test.yaml exec -T api-test \
	python -m pytest -v "${api_smoke_tests[@]}"
