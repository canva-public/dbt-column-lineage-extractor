import argparse
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

def main():
    parser = argparse.ArgumentParser(description="Recursive DBT Column Lineage Extractor CLI")
    parser.add_argument('--model', required=True, help='Model node to find lineage for, e.g. model.jaffle_shop.stg_orders')
    parser.add_argument('--column', required=True, help='Column name to find lineage for, e.g. order_id')
    parser.add_argument('--lineage-parents-file', default='./outputs/lineage_to_direct_parents.json',                        help='Path to the lineage_to_direct_parents.json file, default to ./outputs/lineage_to_direct_parents.json')
    parser.add_argument('--lineage-children-file', default='./outputs/lineage_to_direct_children.json',                        help='Path to the lineage_to_direct_children.json file, default to ./outputs/lineage_to_direct_children.json')


    args = parser.parse_args()

    # utils.clear_screen()

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

    print("========================================")
    print(
        "You can use the structured ancestors and descendants to programmatically use the lineage, "
        "such as for impact analysis, data tagging, etc."
    )
    print(
        "Or, you can copy the json outputs to tools like https://github.com/AykutSarac/jsoncrack.com, "
        "https://jsoncrack.com/editor to visualize the lineage"
    )

if __name__ == '__main__':
    main()
