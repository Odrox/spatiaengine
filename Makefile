# Variable pour les arguments supplémentaires passés à main.py
# Utilisation : make run ARGS="--log-level DEBUG"
ARGS := 

# Cible pour exécuter l'application principale de SpatiaEngine
run:
	@echo "Lancement de SpatiaEngine via Conda avec les arguments : $(ARGS)"
	@if [ -z "$$QGIS_PREFIX_PATH" ]; then \
		echo "Error: QGIS_PREFIX_PATH environment variable is not set." >&2; \
		echo "Please set it to your Conda environment path for QGIS." >&2; \
		exit 1; \
	fi
	@echo "==> Using QGIS from: $$QGIS_PREFIX_PATH"
	DYLD_INSERT_LIBRARIES=$$QGIS_PREFIX_PATH/lib/libsqlite3.dylib conda run -n spatiaengine-stable --no-capture-output python main.py $(ARGS)

# Une cible .PHONY indique à make que 'run' n'est pas un fichier
.PHONY: run