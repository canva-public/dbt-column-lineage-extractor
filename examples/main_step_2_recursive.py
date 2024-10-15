import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

utils.clear_screen()

# %%

lineage_to_direct_parents = utils.read_dict_from_file(
   "./outputs/lineage_to_direct_parents.json"
)
lineage_to_direct_children = utils.read_dict_from_file(
    "./outputs/lineage_to_direct_children.json"
)

# a source node example
model = "seed.jaffle_shop.raw_orders"
column = "id"

# # an intermediate node example
# model = "model.jaffle_shop.stg_orders"
# column = "order_id"


print("========================================")
# Find all ancestors for a specific model and column
print(f"Finding all ancestors of {model}.{column}:")
ancestors_squashed = DbtColumnLineageExtractor.find_all_related(lineage_to_direct_parents, model, column)
ancestors_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
    lineage_to_direct_parents, model, column
)


print("---squashed ancestors---")
utils.pretty_print_dict(ancestors_squashed)
print("---structured ancestors---")
utils.pretty_print_dict(ancestors_structured)


print("========================================")
# Find all descendants for a specific model and column
print(f"Finding all descendants of {model}.{column}:")
descendants_squashed = DbtColumnLineageExtractor.find_all_related(
    lineage_to_direct_children, model, column
)
descendants_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
    lineage_to_direct_children, model, column
)

print("---squashed descendants---")
utils.pretty_print_dict(descendants_squashed)
print("---structured descendants---")
utils.pretty_print_dict(descendants_structured)

# %%
print("========================================")
print(
    "You can use the structured ancestors and descendants to programmatically use the lineage, such as for impact analysis, data tagging, etc."
)
print(
    "Or, you can copy the json outputs to tools like https://github.com/AykutSarac/jsoncrack.com, https://jsoncrack.com/editor to visualize the lineage"
)
