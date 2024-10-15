import argparse
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

def main():
    parser = argparse.ArgumentParser(description="DBT Column Lineage Extractor CLI")
    parser.add_argument('--manifest', default='./inputs/manifest.json', help='Path to the manifest.json file, default to ./inputs/manifest.json')
    parser.add_argument('--catalog', default='./inputs/catalog.json', help='Path to the catalog.json file, default to ./inputs/catalog.json')
    parser.add_argument('--dialect', default='snowflake', help='SQL dialect to use, default is snowflake')
    parser.add_argument('--model', nargs='*', default=[], help='List of models to extract lineage for, default to all models')
    parser.add_argument('--output-dir', default='./outputs', help='Directory to write output json files, default to ./outputs')
    parser.add_argument('--show-ui', action='store_true', help='Flag to show lineage outputs in the console')

    args = parser.parse_args()

    # utils.clear_screen()

    extractor = DbtColumnLineageExtractor(
        manifest_path=args.manifest,
        catalog_path=args.catalog,
        selected_models=args.model,
        dialect=args.dialect,
    )

    lineage_map = extractor.build_lineage_map()
    lineage_to_direct_parents = extractor.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
    lineage_to_direct_children = (
        extractor.get_lineage_to_direct_children_from_lineage_to_direct_parents(
            lineage_to_direct_parents
        )
    )

    utils.write_dict_to_file(
        lineage_to_direct_parents, f"{args.output_dir}/lineage_to_direct_parents.json"
    )

    utils.write_dict_to_file(
        lineage_to_direct_children, f"{args.output_dir}/lineage_to_direct_children.json"
    )

    if args.show_ui:
        print("===== Lineage to Direct Parents =====")
        utils.pretty_print_dict(lineage_to_direct_parents)
        print("===== Lineage to Direct Children =====")
        utils.pretty_print_dict(lineage_to_direct_children)

    print("Lineage extraction complete. Output files written to output directory.")

if __name__ == '__main__':
    main()
