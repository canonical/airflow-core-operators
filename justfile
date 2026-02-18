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

clean-charms:
	find charms -name "*.charm" -delete

integration *args: pack-charms
	#!/usr/bin/bash
	pdb_options=$(if [ -n "${debug}" ]; then echo "--pdb"; fi)
	
	export API_SERVER_CHARM_PATH=$(ls -t charms/api-server/*.charm | head -n1)
	export DAG_PROCESSOR_CHARM_PATH=$(ls -t charms/dag-processor/*.charm | head -n1)
	export SCHEDULER_CHARM_PATH=$(ls -t charms/scheduler/*.charm | head -n1)
	export TRIGGERER_CHARM_PATH=$(ls -t charms/triggerer/*.charm | head -n1)
	export JUJU_MODEL=test
	
	uv sync --group integration
	uv run tox -e integration -- ${pdb_options} {{args}}

clean: clean-charms
	#!/usr/bin/bash
	juju destroy-model --force --destroy-storage --no-prompt "${JUJU_MODEL:-test}"

lint:
	uv sync --frozen --group lint
	uv run tox -e lint

format:
	uv sync --frozen --group format
	uv run tox -e format

get-system-state:
	#!/usr/bin/bash
	df -h
	echo "---"
	juju models
	echo "---"
	juju status --model "${JUJU_MODEL:-test}" --format tabular
