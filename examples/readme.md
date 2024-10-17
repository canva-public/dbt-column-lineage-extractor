## dbt Column Lineage Extractor example

1. Place your dbt `manifest.json` and `catalog.json` files in the `inputs` directory.
2. **Customization**:
   - Set your dialect (only tested with `snowflake` so far) in the `main_step_1_direct.py` script.
   - You can specify the scope of the models you want to extract column lineage for by adding them to the `li_selected_model` list, or leave it empty to process all models (recommended).

3. Run the `main_step_1_direct.py` script to extract direct column lineage:
   ```bash
   python main_step_1_direct.py
   ```

4. This will generate **direct** column lineage relationships for all models in the `outputs` directory.
   - `lineage_to_direct_parents.json`
   - `lineage_to_direct_children.json`

#### Analyze Recursive Column Lineage

1. With the output from the direct column lineage step, run the `main_step_2_recursive.py` script to analyze recursive column lineage:

   **Customization**: Change the `model` and `column` variables in `main_step_2_recursive.py` to target different models or columns for recursive column lineage analysis. You don't need to run the direct lineage extraction again if there are no changes in the models.

   ```bash
   python main_step_2_recursive.py
   ```

2. This will generate squashed/structured ancestors and descendants for the specified model and column.
