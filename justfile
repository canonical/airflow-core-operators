set export
set fallback

[private]
default:
	just --list

pack-charms: clean-core-charms
	#!/usr/bin/bash
	for charm_dir in charms/*/; do
		if [ -f "$charm_dir/charmcraft.yaml" ]; then
			echo "Packing charm in $charm_dir"
			cd "$charm_dir" && charmcraft pack && cd - > /dev/null
		fi
	done

clean-core-charms:
	find charms -maxdepth 2 -name "*.charm" -delete

integration *args: clean pack-charms
	#!/usr/bin/bash
	pdb_options=$(if [ -n "${debug}" ]; then echo "--pdb"; fi)

	export API_SERVER_CHARM_PATH=$(ls -t charms/api-server/*.charm | head -n1)
	export DAG_PROCESSOR_CHARM_PATH=$(ls -t charms/dag-processor/*.charm | head -n1)
	export SCHEDULER_CHARM_PATH=$(ls -t charms/scheduler/*.charm | head -n1)
	export TRIGGERER_CHARM_PATH=$(ls -t charms/triggerer/*.charm | head -n1)
	if [ -n "{{args}}" ]; then
		export JUJU_MODEL=test
	fi

	uv sync --group integration
	uv run tox -e integration -- ${pdb_options} {{args}}

clean: clean-core-charms
	#!/usr/bin/bash
	juju destroy-model --force --destroy-storage --no-prompt "${JUJU_MODEL:-test}" || true

lint:
	#!/usr/bin/bash
	set -eux

	uv sync --frozen --group lint
	uv run tox -e lint

	charms=("api-server" "dag-processor" "scheduler" "triggerer")

	for charm in "${charms[@]}"; do
		uv sync --frozen --group lint
		cd ./charms/$charm && uv run tox -e lint && cd -
	done

format:
	#!/usr/bin/bash
	set -eux

	uv sync --frozen --group format
	uv run tox -e format

	charms=("api-server" "dag-processor" "scheduler" "triggerer")

	for charm in "${charms[@]}"; do
		uv sync --frozen --group format
		cd ./charms/$charm && uv run tox -e format && cd -
	done

# Run unit tests for all core charms
unit:
	#!/usr/bin/bash
	set -eux

	charms=("api-server" "dag-processor" "scheduler" "triggerer")

	for charm in "${charms[@]}"; do
		cd ./charms/$charm && uv run tox -e unit && cd -
	done

get-system-state:
	#!/usr/bin/bash
	model="${JUJU_MODEL:-test}"
	df -h
	echo "---"
	juju models
	echo "---"
	juju status --model "$model" --format tabular
	echo "--- juju debug-log"
	juju debug-log --model "$model" --replay --no-tail || true
	echo "--- pods"
	kubectl get pods -n "$model" -o wide || true
	echo "--- pod details (container/image-pull failures surface here)"
	kubectl describe pods -n "$model" || true
	echo "--- events"
	kubectl get events -n "$model" --sort-by=.lastTimestamp || true
