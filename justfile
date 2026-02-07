set export
set fallback

[private]
default:
	just --list

pack-charms:
	#!/usr/bin/bash
	for charm_dir in charms/*/; do
		if [ -f "$charm_dir/charmcraft.yaml" ]; then
			echo "Packing charm in $charm_dir"
			cd "$charm_dir" && charmcraft pack && cd - > /dev/null
		fi
	done

clean-charms:
	find charms -name "*.charm" -delete

integration *args: pack-charms
	#!/usr/bin/bash
	pdb_options=$(if [ -n "${debug}" ]; then echo "--pdb"; fi)
	
	export JUJU_DATA=${JUJU_DATA:-$HOME/.local/share/juju}
	# Export packed charm paths so tests don't pack or search during execution.
	export API_SERVER_CHARM_PATH=$(ls -t charms/api-server/*.charm | head -n1)
	# Export packed charm paths so tests don't pack or search during execution.
	export DAG_PROCESSOR_CHARM_PATH=$(ls -t charms/dag-processor/*.charm | head -n1)
	# Export packed charm paths so tests don't pack or search during execution.
	export SCHEDULER_CHARM_PATH=$(ls -t charms/scheduler/*.charm | head -n1)
	# Export packed charm paths so tests don't pack or search during execution.
	export TRIGGERER_CHARM_PATH=$(ls -t charms/triggerer/*.charm | head -n1)
	
	uv sync --group integration
	# Allow running a single test file when a positional arg is provided.
	uv run tox -e integration -- ${pdb_options} {{args}}

clean: clean-charms
	#!/usr/bin/bash
	juju models --format json 2>/dev/null | jq -r '.models[] | select(.name | startswith("jubilant-")) | .name' | xargs -r -I {} juju destroy-model --force --destroy-storage --no-prompt {}

lint:
	uv sync --group lint
	uv run ruff check tests/integration
	uv run codespell

format:
	uv sync --group format
	uv run ruff format .

get-system-state:
	#!/usr/bin/bash
	df -h
	echo "---"
	juju models
	echo "---"
	juju status --format tabular
