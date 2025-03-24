import argparse
import os
import json
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

def main():
    parser = argparse.ArgumentParser(description="Recursive DBT Column Lineage Extractor CLI")
    parser.add_argument('--model', required=True, help='Model node to find lineage for, e.g. model.jaffle_shop.stg_orders')
    parser.add_argument('--column', required=True, help='Column name to find lineage for, e.g. order_id')
    parser.add_argument('--lineage-parents-file', default='./outputs/lineage_to_direct_parents.json',                        help='Path to the lineage_to_direct_parents.json file, default to ./outputs/lineage_to_direct_parents.json')
    parser.add_argument('--lineage-children-file', default='./outputs/lineage_to_direct_children.json',                        help='Path to the lineage_to_direct_children.json file, default to ./outputs/lineage_to_direct_children.json')
    parser.add_argument('--output-dir', default='./outputs', help='Output directory for lineage files, default to ./outputs')
    parser.add_argument('--show-ui', action='store_true', help='Show web UI for lineage visualization')

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Read lineage data from files
    lineage_to_direct_parents = utils.read_dict_from_file(args.lineage_parents_file)
    lineage_to_direct_children = utils.read_dict_from_file(args.lineage_children_file)

    print("========================================")
    # Find all ancestors for a specific model and column
    print(f"Finding all ancestors of {args.model}.{args.column}:")
    ancestors_squashed = DbtColumnLineageExtractor.find_all_related(lineage_to_direct_parents, args.model, args.column)
    ancestors_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
        lineage_to_direct_parents, args.model, args.column
    )

    print("---squashed ancestors---")
    utils.pretty_print_dict(ancestors_squashed)
    print("---structured ancestors---")
    utils.pretty_print_dict(ancestors_structured)

    # Save ancestors to files
    ancestors_file = os.path.join(args.output_dir, f"{args.model}_{args.column}_ancestors.json")
    with open(ancestors_file, "w") as f:
        json.dump(ancestors_structured, f, indent=4)

    print("========================================")
    # Find all descendants for a specific model and column
    print(f"Finding all descendants of {args.model}.{args.column}:")
    descendants_squashed = DbtColumnLineageExtractor.find_all_related(
        lineage_to_direct_children, args.model, args.column
    )
    descendants_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
        lineage_to_direct_children, args.model, args.column
    )

    print("---squashed descendants---")
    utils.pretty_print_dict(descendants_squashed)
    print("---structured descendants---")
    utils.pretty_print_dict(descendants_structured)

    # Save descendants to files
    descendants_file = os.path.join(args.output_dir, f"{args.model}_{args.column}_descendants.json")
    with open(descendants_file, "w") as f:
        json.dump(descendants_structured, f, indent=4)

    print("========================================")
    print(
        "You can use the structured ancestors and descendants to programmatically use the lineage, "
        "such as for impact analysis, data tagging, etc."
    )
    print(
        "Or, you can copy the json outputs to tools like https://github.com/AykutSarac/jsoncrack.com, "
        "https://jsoncrack.com/editor to visualize the lineage"
    )
    print(f"Lineage outputs saved to {ancestors_file} and {descendants_file}")

if __name__ == '__main__':
    main()
