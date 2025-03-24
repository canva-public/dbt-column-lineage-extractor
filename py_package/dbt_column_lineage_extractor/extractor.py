import warnings
from sqlglot.lineage import lineage, maybe_parse, SqlglotError, exp
from . import utils
from multiprocessing import Pool, cpu_count, TimeoutError
from concurrent.futures import ThreadPoolExecutor
import traceback
import threading


class DbtColumnLineageExtractor:
    def __init__(self, manifest_path, catalog_path, selected_models=[], dialect="snowflake", n_processes=None, n_threads=None, timeout=3600):
        self.manifest = utils.read_json(manifest_path)
        self.catalog = utils.read_json(catalog_path)
        self.schema_dict = self._generate_schema_dict_from_catalog()
        self.node_mapping = self._get_dict_mapping_full_table_name_to_dbt_node()
        self.dialect = dialect
        self.n_processes = n_processes if n_processes is not None else max(1, cpu_count() - 1)
        self.n_threads = n_threads if n_threads is not None else min(32, self.n_processes * 4)  # Default to 4x processes, max 32
        self.timeout = timeout  # Default timeout of 1 hour
        self._thread_local = threading.local()

        if not selected_models:
            # only select models from manifest nodes
            self.selected_models = [
                x
                for x in self.manifest["nodes"].keys()
                if self.manifest["nodes"][x]["resource_type"] == "model"
            ]
        else:
            self.selected_models = selected_models

    def _generate_schema_dict_from_catalog(self, catalog=None):
        if not catalog:
            catalog = self.catalog
        schema_dict = {}

        def add_to_schema_dict(node):
            dbt_node = DBTNodeCatalog(node)
            db_name, schema_name, table_name = dbt_node.database, dbt_node.schema, dbt_node.name

            if db_name not in schema_dict:
                schema_dict[db_name] = {}
            if schema_name not in schema_dict[db_name]:
                schema_dict[db_name][schema_name] = {}
            if table_name not in schema_dict[db_name][schema_name]:
                schema_dict[db_name][schema_name][table_name] = {}

            schema_dict[db_name][schema_name][table_name].update(dbt_node.get_column_types())

        for node in catalog.get("nodes", {}).values():
            add_to_schema_dict(node)

        for node in catalog.get("sources", {}).values():
            add_to_schema_dict(node)

        return schema_dict

    def _get_dict_mapping_full_table_name_to_dbt_node(self):
        mapping = {}
        for key, node in self.manifest["nodes"].items():
            dbt_node = DBTNodeManifest(node)
            mapping[dbt_node.full_table_name] = key
        for key, node in self.manifest["sources"].items():
            dbt_node = DBTNodeManifest(node)
            mapping[dbt_node.full_table_name] = key
        return mapping

    def _get_list_of_columns_for_a_dbt_node(self, node):
        if node in self.catalog["nodes"]:
            columns = self.catalog["nodes"][node]["columns"]
        elif node in self.catalog["sources"]:
            columns = self.catalog["sources"][node]["columns"]
        else:
            warnings.warn(f"Node {node} not found in catalog, maybe it's not materialized")
            return []
        return list(columns.keys())

    def _get_parent_nodes_catalog(self, model_info):
        parent_nodes = model_info["depends_on"]["nodes"]
        parent_catalog = {"nodes": {}, "sources": {}}
        for parent in parent_nodes:
            if parent in self.catalog["nodes"]:
                parent_catalog["nodes"][parent] = self.catalog["nodes"][parent]
            elif parent in self.catalog["sources"]:
                parent_catalog["sources"][parent] = self.catalog["sources"][parent]
            else:
                warnings.warn(f"Parent model {parent} not found in catalog")
        return parent_catalog

    def _get_thread_local_schema(self):
        if not hasattr(self._thread_local, 'schema_dict'):
            self._thread_local.schema_dict = self.schema_dict.copy()
        return self._thread_local.schema_dict

    @staticmethod
    def _process_model_static(args):
        try:
            model_data, selected_models, catalog, manifest, dialect = args
            model_node, model_info = model_data
            
            if selected_models and model_node not in selected_models:
                return model_node, {}

            if model_info["path"].endswith(".py"):
                print(f"Skipping column lineage detection for Python model {model_node}")
                return model_node, {}
                
            if model_info["resource_type"] != "model":
                print(
                    f"Skipping column lineage detection for {model_node} as it's not a model but a {model_info['resource_type']}"
                )
                return model_node, {}

            # Get parent nodes catalog
            parent_nodes = model_info["depends_on"]["nodes"]
            parent_catalog = {"nodes": {}, "sources": {}}
            for parent in parent_nodes:
                if parent in catalog["nodes"]:
                    parent_catalog["nodes"][parent] = catalog["nodes"][parent]
                elif parent in catalog["sources"]:
                    parent_catalog["sources"][parent] = catalog["sources"][parent]
                else:
                    warnings.warn(f"Parent model {parent} not found in catalog")

            # Get columns
            if model_node in catalog["nodes"]:
                columns = catalog["nodes"][model_node]["columns"]
            elif model_node in catalog["sources"]:
                columns = catalog["sources"][model_node]["columns"]
            else:
                warnings.warn(f"Node {model_node} not found in catalog, maybe it's not materialized")
                return model_node, {}
            columns = list(columns.keys())

            # Extract lineage
            model_sql = model_info["compiled_code"]
            lineage_map = {}
            if not columns:
                parsed_sql = maybe_parse(model_sql, dialect=dialect)
                columns = [
                    column.name if isinstance(column, exp.Column) else column.alias
                    for select in parsed_sql.find_all(exp.Select)
                    for column in select.expressions
                    if isinstance(column, (exp.Column, exp.Alias))
                ]

            for column_name in columns:
                try:
                    lineage_node = lineage(column_name, model_sql, schema=schema_dict, dialect=dialect)
                    lineage_map[column_name] = lineage_node
                except SqlglotError as e:
                    print(f"Error processing model {model_node}, column {column_name}: {e}")

            return model_node, lineage_map
        except Exception as e:
            print(f"Error processing model {model_data[0] if model_data else 'unknown'}: {str(e)}")
            print(traceback.format_exc())
            return model_data[0] if model_data else 'unknown', {}

    def _process_columns_parallel(self, model_node, model_sql, columns, schema_dict):
        """Process columns in parallel using threads"""
        lineage_map = {}
        
        def process_column(column_name):
            try:
                lineage_node = lineage(column_name, model_sql, schema=schema_dict, dialect=self.dialect)
                return column_name, lineage_node
            except SqlglotError as e:
                print(f"Error processing model {model_node}, column {column_name}: {e}")
                return column_name, None

        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            futures = [executor.submit(process_column, column_name) for column_name in columns]
            for future in futures:
                column_name, lineage_node = future.result()
                if lineage_node:
                    lineage_map[column_name] = lineage_node

        return lineage_map

    def build_lineage_map(self):
        lineage_map = {}
        models_to_process = [(node, info) for node, info in self.manifest["nodes"].items()]
        total_models = len(models_to_process)
        failed_models = []
        
        print(f"Processing {total_models} models using {self.n_processes} processes and {self.n_threads} threads per process")
        
        # Prepare arguments for each model
        process_args = [
            (model_data, self.selected_models, self.catalog, self.manifest, self.dialect)
            for model_data in models_to_process
        ]
        
        try:
            with Pool(processes=self.n_processes) as pool:
                for i, (model_node, model_lineage) in enumerate(
                    pool.imap_unordered(self._process_model_static, process_args), 1
                ):
                    try:
                        print(f"{i}/{total_models} Processed model {model_node}")
                        if model_lineage:  # Only add non-empty lineage maps
                            lineage_map[model_node] = model_lineage
                    except Exception as e:
                        print(f"Error processing results for model {model_node}: {str(e)}")
                        failed_models.append(model_node)
                        continue

        except TimeoutError:
            print(f"Processing timed out after {self.timeout} seconds")
        except Exception as e:
            print(f"Error in parallel processing: {str(e)}")
            print(traceback.format_exc())
        finally:
            if failed_models:
                print(f"\nWarning: Failed to process {len(failed_models)} models:")
                for model in failed_models:
                    print(f"  - {model}")

        return lineage_map

    def get_dbt_node_from_sqlglot_table_node(self, node):
        if node.source.key != "table":
            raise ValueError(f"Node source is not a table, but {node.source.key}")
        column_name = node.name.split(".")[-1].lower()
        table_name = f"{node.source.catalog}.{node.source.db}.{node.source.name}"
        table_name = table_name.lower()

        if table_name in self.node_mapping:
            dbt_node = self.node_mapping[table_name].lower()
        else:
            warnings.warn(f"Table {table_name} not found in node mapping")
            dbt_node = f"_NOT_FOUND___{table_name.lower()}"
            # raise ValueError(f"Table {table_name} not found in node mapping")

        return {"column": column_name, "dbt_node": dbt_node}

    def get_columns_lineage_from_sqlglot_lineage_map(self, lineage_map, picked_columns=[]):
        columns_lineage = {key.lower(): {} for key in self.selected_models}

        for model_node, columns in lineage_map.items():
            model_node = model_node.lower()
            for column, node in columns.items():
                column = column.lower()
                if picked_columns and column not in picked_columns:
                    continue

                columns_lineage[model_node][column] = []
                for n in node.walk():
                    if n.source.key == "table":
                        parent_columns = self.get_dbt_node_from_sqlglot_table_node(n)
                        if (
                            parent_columns["dbt_node"] != model_node
                            and parent_columns not in columns_lineage[model_node][column]
                        ):
                            columns_lineage[model_node][column].append(parent_columns)
                if not columns_lineage[model_node][column]:
                    warnings.warn(f"No lineage found for {model_node} - {column}")
        return columns_lineage

    def get_lineage_to_direct_children_from_lineage_to_direct_parents(
        self, lineage_to_direct_parents
    ):
        children_lineage = {}
        
        def process_parent(parent_data):
            parent_model, parent_column, child_data = parent_data
            if parent_model not in children_lineage:
                children_lineage[parent_model] = {}
            if parent_column not in children_lineage[parent_model]:
                children_lineage[parent_model][parent_column] = []
            children_lineage[parent_model][parent_column].append(child_data)

        # Prepare data for parallel processing
        parent_data_list = []
        for child_model, columns in lineage_to_direct_parents.items():
            child_model = child_model.lower()
            for child_column, parents in columns.items():
                child_column = child_column.lower()
                for parent in parents:
                    parent_model = parent["dbt_node"].lower()
                    parent_column = parent["column"].lower()
                    parent_data_list.append((
                        parent_model,
                        parent_column,
                        {"column": child_column, "dbt_node": child_model}
                    ))

        # Process parents in parallel using threads
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            executor.map(process_parent, parent_data_list)

        return children_lineage

    @staticmethod
    def find_all_related(lineage_map, model_node, column, visited=None):
        column = column.lower()
        model_node = model_node.lower()
        if visited is None:
            visited = set()
        related = {}
        if model_node in lineage_map and column in lineage_map[model_node]:
            for related_node in lineage_map[model_node][column]:
                related_model = related_node["dbt_node"]
                related_column = related_node["column"]
                if (related_model, related_column) not in visited:
                    visited.add((related_model, related_column))
                    if related_model not in related:
                        related[related_model] = []
                    related[related_model].append(related_column)
                    further_related = DbtColumnLineageExtractor.find_all_related(
                        lineage_map, related_model, related_column, visited
                    )
                    for further_model, further_columns in further_related.items():
                        if further_model not in related:
                            related[further_model] = []
                        related[further_model].extend(further_columns)
        return related

    @staticmethod
    def find_all_related_with_structure(lineage_map, model_node, column, visited=None):
        model_node = model_node.lower()
        column = column.lower()
        if visited is None:
            visited = set()

        # Initialize the related structure for the current node and column.
        related_structure = {}

        if model_node in lineage_map and column in lineage_map[model_node]:
            related_structure = {}  # Start with an empty dict for this level

            for related_node in lineage_map[model_node][column]:
                related_model = related_node["dbt_node"]
                related_column = related_node["column"]

                if (related_model, related_column) not in visited:
                    visited.add((related_model, related_column))

                    # Recursively get the structure for each related node
                    subsequent_structure = (
                        DbtColumnLineageExtractor.find_all_related_with_structure(
                            lineage_map, related_model, related_column, visited
                        )
                    )

                    # Use a structure to show relationships distinctly
                    if related_model not in related_structure:
                        related_structure[related_model] = {}

                    # Add information about the column lineage
                    related_structure[related_model][related_column] = {"+": subsequent_structure}

        return related_structure


class DBTNodeCatalog:
    def __init__(self, node_data):
        self.database = node_data["metadata"]["database"]
        self.schema = node_data["metadata"]["schema"]
        self.name = node_data["metadata"]["name"]
        self.columns = node_data["columns"]

    @property
    def full_table_name(self):
        return f"{self.database}.{self.schema}.{self.name}".lower()

    def get_column_types(self):
        return {col_name: col_info["type"] for col_name, col_info in self.columns.items()}


class DBTNodeManifest:
    def __init__(self, node_data):
        self.database = node_data["database"]
        self.schema = node_data["schema"]
        self.name = node_data["name"]
        self.columns = node_data["columns"]

    @property
    def full_table_name(self):
        return f"{self.database}.{self.schema}.{self.name}".lower()


# TODO: add metadata columns to external tables
