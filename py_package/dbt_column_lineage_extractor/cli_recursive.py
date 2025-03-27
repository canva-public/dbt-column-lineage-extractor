import argparse
import os
import json
import webbrowser
from pathlib import Path
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

def create_html_viewer(mermaid_code, output_dir, model_node, column):
    """Create an HTML file with Mermaid viewer."""
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DBT Column Lineage: {model_node}.{column}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'default',
            flowchart: {{
                useMaxWidth: false,
                htmlLabels: true,
                curve: 'basis'
            }}
        }});
    </script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .mermaid {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .controls {{
            margin-bottom: 20px;
        }}
        button {{
            padding: 8px 16px;
            margin-right: 10px;
            border: none;
            border-radius: 4px;
            background-color: #4CAF50;
            color: white;
            cursor: pointer;
        }}
        button:hover {{
            background-color: #45a049;
        }}
        .title {{
            margin-bottom: 20px;
            color: #333;
        }}
    </style>
</head>
<body>
    <h1 class="title">Column Lineage: {model_node}.{column}</h1>
    <div class="controls">
        <button onclick="zoomIn()">Zoom In</button>
        <button onclick="zoomOut()">Zoom Out</button>
        <button onclick="resetZoom()">Reset Zoom</button>
    </div>
    <div class="mermaid">
{mermaid_code}
    </div>
    <script>
        let scale = 1;
        const diagram = document.querySelector('.mermaid');
        
        function zoomIn() {{
            scale *= 1.2;
            diagram.style.transform = `scale(${{scale}})`;
            diagram.style.transformOrigin = 'top left';
        }}
        
        function zoomOut() {{
            scale *= 0.8;
            diagram.style.transform = `scale(${{scale}})`;
            diagram.style.transformOrigin = 'top left';
        }}
        
        function resetZoom() {{
            scale = 1;
            diagram.style.transform = `scale(${{scale}})`;
            diagram.style.transformOrigin = 'top left';
        }}
    </script>
</body>
</html>
"""
    # Create a filename based on model and column
    safe_model_name = model_node.replace('.', '_').replace('/', '_')
    safe_column_name = column.replace('.', '_').replace('/', '_')
    viewer_file = os.path.join(output_dir, f"{safe_model_name}__{safe_column_name}.html")
    
    with open(viewer_file, 'w') as f:
        f.write(html_content)
    return viewer_file

def convert_to_mermaid(model_node, column, ancestors_structured, descendants_structured):
    """Convert lineage data to Mermaid flowchart format."""
    mermaid_lines = [
        "flowchart TD",
        "    %% Styles",
        "    classDef model fill:#e3f2fd,stroke:#1565c0",
        "    classDef column fill:#f3e5f5,stroke:#6a1b9a",
        "    classDef ancestorModel fill:#e1f5fe,stroke:#01579b",
        "    classDef ancestorColumn fill:#e8f5e9,stroke:#1b5e20",
        "    classDef descendantModel fill:#f3e5f5,stroke:#4a148c",
        "    classDef descendantColumn fill:#fce4ec,stroke:#880e4f",
        "    classDef centralModel fill:#fff9c4,stroke:#f57f17,stroke-width:3px",
        "    classDef centralColumn fill:#ffecb3,stroke:#ff6f00,stroke-width:2px",
        "    %% Link styles",
        "    linkStyle default stroke:#666,stroke-width:2px",
        "    %% Nodes and relationships"
    ]
    visited_models = set()
    visited_columns = set()
    model_relationships = set()  # Track model relationships to avoid duplicates
    link_count = 0  # Counter for link styles
    
    def get_model_name(full_node):
        """Extract model name from full node path."""
        return full_node.split('.')[-1]
    
    def add_model_and_column(node, col, direction=None):
        """Add model and column nodes with appropriate styling."""
        nonlocal link_count
        model_id = f"model_{node}".replace('.', '_')
        column_id = f"{node}_{col}".replace('.', '_')
        model_label = get_model_name(node)
        
        # Add model node if not visited
        if model_id not in visited_models:
            mermaid_lines.append(f"    {model_id}[{model_label}]")
            visited_models.add(model_id)
            # Apply style based on direction
            if direction == 'up':
                mermaid_lines.append(f"    class {model_id} ancestorModel")
            elif direction == 'down':
                mermaid_lines.append(f"    class {model_id} descendantModel")
            else:
                mermaid_lines.append(f"    class {model_id} model")
        
        # Add column node if not visited
        if column_id not in visited_columns:
            mermaid_lines.append(f"    {column_id}{{'{col}'}}") # Use curly braces for column nodes
            visited_columns.add(column_id)
            # Apply style based on direction
            if direction == 'up':
                mermaid_lines.append(f"    class {column_id} ancestorColumn")
            elif direction == 'down':
                mermaid_lines.append(f"    class {column_id} descendantColumn")
            else:
                mermaid_lines.append(f"    class {column_id} column")
        
        # Connect model to column with a dotted line
        mermaid_lines.append(f"    {model_id} -.- {column_id}")
        # Style the connection line to be thinner and lighter
        mermaid_lines.append(f"    linkStyle {link_count} stroke:#999,stroke-width:1px,stroke-dasharray:3")
        link_count += 1
        
        return model_id, column_id
    
    def process_structure(structure, direction='up', parent_ids=None):
        """Process the structured lineage data recursively."""
        nonlocal link_count
        current_level_ids = []
        
        for node, columns in structure.items():
            for col, details in columns.items():
                # Add source model and column nodes
                source_model_id, source_column_id = add_model_and_column(node, col, direction)
                current_level_ids.extend([source_model_id, source_column_id])
                
                # Connect to parent level if exists
                if parent_ids:
                    for parent_model_id, parent_column_id in zip(parent_ids[::2], parent_ids[1::2]):
                        # Add relationship between models with solid arrow if not already added
                        model_rel = (source_model_id, parent_model_id) if direction == 'down' else (parent_model_id, source_model_id)
                        if model_rel not in model_relationships:
                            if direction == 'up':
                                mermaid_lines.append(f"    {source_model_id} --> {parent_model_id}")
                            else:
                                mermaid_lines.append(f"    {parent_model_id} --> {source_model_id}")
                            # Style the model relationship
                            mermaid_lines.append(f"    linkStyle {link_count} stroke:#333,stroke-width:2px")
                            link_count += 1
                            model_relationships.add(model_rel)
                        
                        # Add relationship between columns with dashed arrow
                        if direction == 'up':
                            mermaid_lines.append(f"    {source_column_id} -.-> {parent_column_id}")
                        else:
                            mermaid_lines.append(f"    {parent_column_id} -.-> {source_column_id}")
                        # Style the column relationship
                        mermaid_lines.append(f"    linkStyle {link_count} stroke:#666,stroke-width:1.5px")
                        link_count += 1
                
                # Process nested relationships
                if '+' in details:
                    nested_ids = []
                    for nested_node, nested_cols in details['+'].items():
                        for nested_col in nested_cols:
                            # Add target model and column nodes
                            target_model_id, target_column_id = add_model_and_column(nested_node, nested_col, direction)
                            nested_ids.extend([target_model_id, target_column_id])
                            
                            # Add relationship between models with solid arrow if not already added
                            model_rel = (target_model_id, source_model_id) if direction == 'up' else (source_model_id, target_model_id)
                            if model_rel not in model_relationships:
                                if direction == 'up':
                                    mermaid_lines.append(f"    {target_model_id} --> {source_model_id}")
                                else:
                                    mermaid_lines.append(f"    {source_model_id} --> {target_model_id}")
                                # Style the model relationship
                                mermaid_lines.append(f"    linkStyle {link_count} stroke:#333,stroke-width:2px")
                                link_count += 1
                                model_relationships.add(model_rel)
                            
                            # Add relationship between columns with dashed arrow
                            if direction == 'up':
                                mermaid_lines.append(f"    {target_column_id} -.-> {source_column_id}")
                            else:
                                mermaid_lines.append(f"    {source_column_id} -.-> {target_column_id}")
                            # Style the column relationship
                            mermaid_lines.append(f"    linkStyle {link_count} stroke:#666,stroke-width:1.5px")
                            link_count += 1
                            
                            # Process nested structure recursively
                            if nested_cols[nested_col].get('+'):
                                process_structure({nested_node: {nested_col: nested_cols[nested_col]}}, direction, [target_model_id, target_column_id])
                    
                    if nested_ids:
                        current_level_ids.extend(nested_ids)
        
        return current_level_ids
    
    # Add the central model and column nodes first
    central_model_id = f"model_{model_node}".replace('.', '_')
    central_column_id = f"{model_node}_{column}".replace('.', '_')
    model_label = get_model_name(model_node)
    
    mermaid_lines.append(f"    {central_model_id}[{model_label}]")
    mermaid_lines.append(f"    {central_column_id}{{{column}}}")
    mermaid_lines.append(f"    {central_model_id} -.- {central_column_id}")
    # Style the central connection
    mermaid_lines.append(f"    linkStyle {link_count} stroke:#f57f17,stroke-width:1px,stroke-dasharray:3")
    link_count += 1
    
    mermaid_lines.append(f"    class {central_model_id} centralModel")
    mermaid_lines.append(f"    class {central_column_id} centralColumn")
    
    visited_models.add(central_model_id)
    visited_columns.add(central_column_id)
    
    # Process ancestors and connect them to central node
    if ancestors_structured:
        ancestor_ids = process_structure(ancestors_structured, 'up', [central_model_id, central_column_id])
    
    # Process descendants and connect them to central node
    if descendants_structured:
        descendant_ids = process_structure(descendants_structured, 'down', [central_model_id, central_column_id])
    
    return "\n".join(mermaid_lines)

def find_potential_matches(lineage_data, model_name):
    """Find potential matches for a model name in lineage data."""
    matches = []
    search_term = model_name.lower()
    for node in lineage_data.keys():
        node_lower = node.lower()
        # Check if the node contains the search term
        if search_term in node_lower:
            matches.append(node)
    return matches

def main():
    parser = argparse.ArgumentParser(description="Recursive DBT Column Lineage Extractor CLI")
    parser.add_argument('--model', required=True, help='Model to find lineage for, must be full node path (e.g. "model.jaffle_shop.customers")')
    parser.add_argument('--column', required=True, help='Column name to find lineage for, e.g. order_id')
    parser.add_argument('--lineage-parents-file', default='./outputs/lineage_to_direct_parents.json',
                        help='Path to the lineage_to_direct_parents.json file, default to ./outputs/lineage_to_direct_parents.json')
    parser.add_argument('--lineage-children-file', default='./outputs/lineage_to_direct_children.json',
                        help='Path to the lineage_to_direct_children.json file, default to ./outputs/lineage_to_direct_children.json')
    parser.add_argument('--output-dir', default='./outputs', help='Output directory for lineage files, default to ./outputs')
    parser.add_argument('--no-ui', action='store_true', help='Do not automatically open the visualization in browser')
    parser.add_argument('--output-format', choices=['json', 'mermaid', 'both'], default='both',
                        help='Output format for lineage data. Choose between json, mermaid, or both. Default is both.')
    parser.add_argument('--show-details', action='store_true', help='Show detailed squashed/structured ancestors/descendants in terminal')

    args = parser.parse_args()

    try:
        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)

        # Read lineage data from files
        try:
            lineage_to_direct_parents = utils.read_dict_from_file(args.lineage_parents_file)
            lineage_to_direct_children = utils.read_dict_from_file(args.lineage_children_file)
        except FileNotFoundError as e:
            print(f"Error: Could not find required lineage file: {e}")
            print("\nTo generate the required lineage files, please run one of the following commands first:")
            print("\n1. To scan the whole project (takes longer, To scan the whole project (takes longer, but you don't need to run it again for different models if there is no model change):")
            print("   dbt_column_lineage_direct --manifest path/to/manifest.json --catalog path/to/catalog.json")
            print("\n2. If only interested in this model (faster):")
            print(f"   dbt_column_lineage_direct --manifest path/to/manifest.json --catalog path/to/catalog.json --model +{args.model}+")
            print("\nAfter running one of these commands, try this command again.")
            return 1
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in lineage file: {e}")
            return 1

        # Use model name as provided - must be full node path
        model_node = args.model

        # Check if model exists in lineage files
        if model_node not in lineage_to_direct_parents and model_node not in lineage_to_direct_children:
            # Search for potential matches in both lineage files
            parent_matches = find_potential_matches(lineage_to_direct_parents, model_node)
            child_matches = find_potential_matches(lineage_to_direct_children, model_node)
            
            # Combine unique matches
            all_matches = list(set(parent_matches + child_matches))
            
            if all_matches:
                if len(all_matches) > 1:
                    print("\nError: Multiple potential matches found. Please use one of the following full node paths:")
                    for match in sorted(all_matches):
                        print(f"  - {match}")
                    return 1
                else:
                    # Single match found - use it automatically
                    model_node = all_matches[0]
                    print(f"Using model: {model_node}")
            else:
                print(f"No matches found for '{model_node}'. Results may be empty or incomplete.")

        print("========================================")
        # Find all ancestors for a specific model and column
        print(f"Finding all ancestors of {model_node}.{args.column}:")
        ancestors_squashed = DbtColumnLineageExtractor.find_all_related(lineage_to_direct_parents, model_node, args.column)
        ancestors_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_to_direct_parents, model_node, args.column
        )

        if args.show_details:
            print("---squashed ancestors---")
            utils.pretty_print_dict(ancestors_squashed)
            print("---structured ancestors---")
            utils.pretty_print_dict(ancestors_structured)

        # Find all descendants for a specific model and column
        print("========================================")
        print(f"Finding all descendants of {model_node}.{args.column}:")
        descendants_squashed = DbtColumnLineageExtractor.find_all_related(
            lineage_to_direct_children, model_node, args.column
        )
        descendants_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_to_direct_children, model_node, args.column
        )

        if args.show_details:
            print("---squashed descendants---")
            utils.pretty_print_dict(descendants_squashed)
            print("---structured descendants---")
            utils.pretty_print_dict(descendants_structured)

        # Save outputs based on format
        if args.output_format in ['json', 'both']:
            # Create safe filenames by replacing dots with underscores
            safe_model_name = model_node.replace('.', '_')
            
            # Save ancestors to files
            ancestors_file = os.path.join(args.output_dir, f"{safe_model_name}_{args.column}_ancestors.json")
            utils.write_dict_to_file(ancestors_structured, ancestors_file)
            
            # Save descendants to files
            descendants_file = os.path.join(args.output_dir, f"{safe_model_name}_{args.column}_descendants.json")
            utils.write_dict_to_file(descendants_structured, descendants_file)
            
            print("========================================")
            print(f"Lineage outputs saved to {ancestors_file} and {descendants_file}")

        if args.output_format in ['mermaid', 'both']:
            # Convert to Mermaid format
            mermaid_output = convert_to_mermaid(model_node, args.column, ancestors_structured, descendants_structured)
            
            # Save Mermaid output
            mermaid_file = os.path.join(args.output_dir, f"{model_node}_{args.column}_lineage.mmd")
            with open(mermaid_file, 'w') as f:
                f.write(mermaid_output)
            
            # Always create HTML viewer for Mermaid output
            viewer_file = create_html_viewer(mermaid_output, args.output_dir, model_node, args.column)
            
            print(f"Mermaid output saved to {mermaid_file}")
            print(f"HTML viewer created at: {viewer_file}")
            
            # Open the viewer by default unless --no-ui is specified
            if not args.no_ui:
                print("Opening Mermaid diagram in local viewer...")
                webbrowser.open(f"file://{os.path.abspath(viewer_file)}")
        
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main())
