import pytest
import os
import json
from dbt_column_lineage_extractor import DbtColumnLineageExtractor
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_catalog():
    return {
        "nodes": {},
        "sources": {}
    }

class TestSelectors:
    """Tests for the dbt-style selector functionality."""

    @pytest.fixture
    def extractor(self):
        """Create an extractor using real test data files."""
        return DbtColumnLineageExtractor(
            manifest_path="tests/test_data/inputs/manifest.json",
            catalog_path="tests/test_data/inputs/catalog.json",
            selected_models=[],
            dialect="snowflake"
        )
    
    @pytest.fixture
    def model_info(self, extractor):
        """Extract key information about the test models for verification."""
        # This will help us understand the test data structure
        models = {}
        sources = {}
        
        # Get all models
        for node_id, node in extractor.manifest["nodes"].items():
            if node.get("resource_type") == "model":
                models[node_id] = {
                    "name": node.get("name"),
                    "tags": node.get("tags", []),
                    "package": node.get("package_name", ""),
                    "path": node.get("path", ""),
                    "parents": extractor.parent_map.get(node_id, []),
                    "children": extractor.child_map.get(node_id, [])
                }
        
        # Get sources with more detailed information
        for node_id, node in extractor.manifest.get("sources", {}).items():
            sources[node_id] = {
                "name": node.get("name"),
                "tags": node.get("tags", []),
                "package": node.get("package_name", ""),
                "path": node.get("path", ""),
                "parents": extractor.parent_map.get(node_id, []),
                "children": extractor.child_map.get(node_id, [])
            }
            
        return {"models": models, "sources": sources}

    def test_all_models_selection(self, extractor):
        """Test that empty selector selects all models."""
        # Default behavior when initializing with empty list
        assert len(extractor.selected_models) > 0
        # All selected nodes should be models (not enforcing model. prefix since we now support sources)
        assert all(node.startswith("model.") or node.startswith("source.") for node in extractor.selected_models)

    def test_specific_model_selection(self, extractor, model_info):
        """Test selecting a specific model."""
        # Get a model ID from the available models
        model_id = list(model_info["models"].keys())[0]
        
        # Select just that model
        extractor.selected_models = extractor._parse_selectors([model_id])
        
        assert len(extractor.selected_models) == 1
        assert model_id in extractor.selected_models

    def test_model_name_without_prefix(self, extractor, model_info):
        """Test selecting models by name without resource prefix."""
        # Get a model name from the available models
        model_id = list(model_info["models"].keys())[0]
        model_name = model_info["models"][model_id]["name"]
        
        # Select by just the name without the full ID
        extractor.selected_models = extractor._parse_selectors([model_name])
        
        # Find all models and sources with this name (could be more than one)
        expected_nodes = [
            node_id for node_id, info in model_info["models"].items()
            if info["name"] == model_name
        ]
        expected_nodes.extend([
            node_id for node_id, info in model_info["sources"].items()
            if info["name"] == model_name
        ])
        
        # Verify all expected models are selected
        assert len(extractor.selected_models) >= 1
        for node in expected_nodes:
            assert node in extractor.selected_models

    def test_ancestor_selection(self, extractor, model_info):
        """Test selecting a model and its ancestors."""
        # Find a model with ancestors
        for model_id, info in model_info["models"].items():
            if info["parents"]:
                # Select this model and all its ancestors
                selector = f"+{info['name']}"
                extractor.selected_models = extractor._parse_selectors([selector])
                
                # Verify the model itself is selected
                assert any(node_id in extractor.selected_models for node_id, node_info in model_info["models"].items() 
                           if node_info["name"] == info["name"]) or any(node_id in extractor.selected_models for node_id, node_info in model_info["sources"].items() 
                           if node_info["name"] == info["name"])
                
                # Verify at least one parent is included
                for parent in info["parents"]:
                    if parent in model_info["models"] or parent in model_info["sources"]:  # Check both models and sources
                        assert parent in extractor.selected_models
                
                # Only need to test one model
                break

    def test_descendant_selection(self, extractor, model_info):
        """Test selecting a model and its descendants."""
        # Find a model with descendants
        for model_id, info in model_info["models"].items():
            if info["children"]:
                # Select this model and all its descendants
                selector = f"{info['name']}+"
                extractor.selected_models = extractor._parse_selectors([selector])
                
                # Verify the model itself is selected
                assert any(node_id in extractor.selected_models for node_id, node_info in model_info["models"].items() 
                           if node_info["name"] == info["name"]) or any(node_id in extractor.selected_models for node_id, node_info in model_info["sources"].items() 
                           if node_info["name"] == info["name"])
                
                # Verify at least one child is included
                for child in info["children"]:
                    if child in model_info["models"] or child in model_info["sources"]:  # Check both models and sources
                        assert child in extractor.selected_models
                
                # Only need to test one model
                break

    def test_tag_selection(self, extractor, model_info):
        """Test selecting models by tag."""
        # Find tags used in the models and sources
        all_tags = set()
        for info in model_info["models"].values():
            all_tags.update(info.get("tags", []))
        for info in model_info["sources"].values():
            all_tags.update(info.get("tags", []))
        
        if all_tags:
            # Select a tag
            tag = list(all_tags)[0]
            extractor.selected_models = extractor._parse_selectors([f"tag:{tag}"])
            
            # Find models and sources that should be selected
            expected_nodes = [
                node_id for node_id, info in model_info["models"].items()
                if tag in info.get("tags", [])
            ]
            expected_nodes.extend([
                node_id for node_id, info in model_info["sources"].items()
                if tag in info.get("tags", [])
            ])
            
            # Verify selected nodes match expected
            assert len(extractor.selected_models) == len(expected_nodes)
            for node in expected_nodes:
                assert node in extractor.selected_models

    def test_path_selection(self, extractor, model_info):
        """Test selecting models by path."""
        # Find common path elements in the models and sources
        path_elements = set()
        for info in model_info["models"].values():
            if "/" in info.get("path", ""):
                # Extract the directory part of the path
                path_parts = info.get("path", "").split("/")
                if len(path_parts) > 1:
                    path_elements.add(path_parts[0])
        
        # Also check sources for paths
        for info in model_info["sources"].values():
            if "/" in info.get("path", ""):
                path_parts = info.get("path", "").split("/")
                if len(path_parts) > 1:
                    path_elements.add(path_parts[0])
        
        if path_elements:
            # Select a path
            path_element = list(path_elements)[0]
            extractor.selected_models = extractor._parse_selectors([f"path:{path_element}"])
            
            # Find nodes that should be selected
            expected_nodes = [
                node_id for node_id, info in model_info["models"].items()
                if path_element in info.get("path", "")
            ]
            expected_nodes.extend([
                node_id for node_id, info in model_info["sources"].items()
                if path_element in info.get("path", "")
            ])
            
            # Verify at least some nodes are selected
            assert len(extractor.selected_models) > 0
            # Verify all selected nodes contain the path element
            for node in extractor.selected_models:
                if node in model_info["models"]:
                    assert path_element in model_info["models"][node]["path"]
                elif node in model_info["sources"]:
                    assert path_element in model_info["sources"][node]["path"]

    def test_package_selection(self, extractor, model_info):
        """Test selecting models by package."""
        # Find packages used in the models and sources
        packages = set()
        for info in model_info["models"].values():
            if info.get("package"):
                packages.add(info["package"])
        for info in model_info["sources"].values():
            if info.get("package"):
                packages.add(info["package"])
        
        if packages:
            # Select a package
            package = list(packages)[0]
            extractor.selected_models = extractor._parse_selectors([f"package:{package}"])
            
            # Find models and sources that should be selected
            expected_nodes = [
                node_id for node_id, info in model_info["models"].items()
                if info.get("package") == package
            ]
            expected_nodes.extend([
                node_id for node_id, info in model_info["sources"].items()
                if info.get("package") == package
            ])
            
            # Verify selected nodes match expected
            assert len(extractor.selected_models) == len(expected_nodes)
            for node in expected_nodes:
                assert node in extractor.selected_models

    def test_union_selection(self, extractor, model_info):
        """Test selecting models with union operator (space)."""
        # Get two different model or source names
        node_names = []
        
        # Try to get model names first
        for info in model_info["models"].values():
            if info["name"] not in node_names:
                node_names.append(info["name"])
                if len(node_names) >= 2:
                    break
        
        # If we don't have 2 model names yet, try source names
        if len(node_names) < 2:
            for info in model_info["sources"].values():
                if info["name"] not in node_names:
                    node_names.append(info["name"])
                    if len(node_names) >= 2:
                        break
        
        if len(node_names) >= 2:
            # Select both nodes
            selector = f"{node_names[0]} {node_names[1]}"
            extractor.selected_models = extractor._parse_selectors([selector])
            
            # Find all models and sources with either name
            expected_nodes = [
                node_id for node_id, info in model_info["models"].items()
                if info["name"] in node_names
            ]
            expected_nodes.extend([
                node_id for node_id, info in model_info["sources"].items()
                if info["name"] in node_names
            ])
            
            # Verify all expected nodes are selected
            assert len(extractor.selected_models) >= 2
            for node in expected_nodes:
                assert node in extractor.selected_models

    def test_intersection_selection(self, extractor, model_info):
        """Test selecting models with intersection operator (comma)."""
        # Find two tags that have at least one model in common
        tag_to_nodes = {}
        for node_id, info in model_info["models"].items():
            for tag in info.get("tags", []):
                if tag not in tag_to_nodes:
                    tag_to_nodes[tag] = []
                tag_to_nodes[tag].append(node_id)
        
        common_tags = []
        common_nodes = []
        for tag1, nodes1 in tag_to_nodes.items():
            for tag2, nodes2 in tag_to_nodes.items():
                if tag1 != tag2:
                    intersection = set(nodes1) & set(nodes2)
                    if intersection:
                        common_tags = [tag1, tag2]
                        common_nodes = list(intersection)
                        break
            if common_tags:
                break
        
        if common_tags:
            # Select models with both tags
            selector = f"tag:{common_tags[0]},tag:{common_tags[1]}"
            extractor.selected_models = extractor._parse_selectors([selector])
            
            # Verify correct nodes are selected
            assert len(extractor.selected_models) == len(common_nodes)
            for node in common_nodes:
                assert node in extractor.selected_models

    def test_complex_selection(self, extractor, model_info):
        """Test a complex selection combining multiple selector types."""
        # Only run this test if we have tags and packages
        all_tags = set()
        packages = set()
        for info in model_info["models"].values():
            all_tags.update(info.get("tags", []))
            if info.get("package"):
                packages.add(info["package"])
        
        if all_tags and packages:
            tag = list(all_tags)[0]
            package = list(packages)[0]
            
            # Select models that are either:
            # 1. In the selected package, or
            # 2. Tagged with the selected tag
            selector = f"package:{package} tag:{tag}"
            extractor.selected_models = extractor._parse_selectors([selector])
            
            # Find models that should be selected
            expected_nodes = [
                node_id for node_id, info in model_info["models"].items()
                if info.get("package") == package or tag in info.get("tags", [])
            ]
            
            # Verify all expected nodes are selected
            assert len(extractor.selected_models) == len(expected_nodes)
            for node in expected_nodes:
                assert node in extractor.selected_models

    def test_empty_selector(self, extractor):
        """Test empty selector list."""
        extractor.selected_models = extractor._parse_selectors([])
        assert len(extractor.selected_models) == 0

    def test_nonexistent_model(self, extractor):
        """Test selecting a model that doesn't exist."""
        extractor.selected_models = extractor._parse_selectors(["nonexistent_model"])
        assert len(extractor.selected_models) == 0

    def test_ancestors_of_leaf_node(self, extractor, model_info):
        """Test selecting ancestors of a leaf node (no ancestors)."""
        # Find a model or source with no parents
        leaf_nodes = [
            node_id for node_id, info in model_info["models"].items()
            if not info["parents"] or all(parent not in model_info["models"] and parent not in model_info["sources"] for parent in info["parents"])
        ]
        leaf_nodes.extend([
            node_id for node_id, info in model_info["sources"].items()
            if not info["parents"] or all(parent not in model_info["models"] and parent not in model_info["sources"] for parent in info["parents"])
        ])
        
        if leaf_nodes:
            leaf_node = leaf_nodes[0]
            if leaf_node in model_info["models"]:
                leaf_name = model_info["models"][leaf_node]["name"]
            else:
                leaf_name = model_info["sources"][leaf_node]["name"]
        
            # Select ancestors of a leaf node
            extractor.selected_models = extractor._parse_selectors([f"+{leaf_name}"])
        
            # Should only include the leaf itself since it has no ancestors
            assert len(extractor.selected_models) >= 1
            assert leaf_node in extractor.selected_models

    def test_descendants_of_leaf_node(self, extractor, model_info):
        """Test selecting descendants of a leaf node (no descendants)."""
        # Find a model or source with no children
        leaf_nodes = [
            node_id for node_id, info in model_info["models"].items()
            if not info["children"] or all(child not in model_info["models"] and child not in model_info["sources"] for child in info["children"])
        ]
        leaf_nodes.extend([
            node_id for node_id, info in model_info["sources"].items()
            if not info["children"] or all(child not in model_info["models"] and child not in model_info["sources"] for child in info["children"])
        ])
        
        if leaf_nodes:
            leaf_node = leaf_nodes[0]
            if leaf_node in model_info["models"]:
                leaf_name = model_info["models"][leaf_node]["name"]
            else:
                leaf_name = model_info["sources"][leaf_node]["name"]
        
            # Select descendants of a leaf node
            extractor.selected_models = extractor._parse_selectors([f"{leaf_name}+"])
        
            # Should only include the leaf itself since it has no descendants
            assert len(extractor.selected_models) >= 1
            assert leaf_node in extractor.selected_models

    def test_recursive_graph_traversal(self, extractor, model_info):
        """Test that graph traversal handles recursive relationships properly."""
        # Find a model with at least one child and one grandchild
        for model_id, info in model_info["models"].items():
            if info["children"]:
                for child in info["children"]:
                    if (child in model_info["models"] and model_info["models"][child]["children"]) or \
                       (child in model_info["sources"] and model_info["sources"][child]["children"]):
                        # We've found a model with at least a child and grandchild
                        model_name = info["name"]
    
                        # Get all descendants
                        extractor.selected_models = extractor._parse_selectors([f"{model_name}+"])
    
                        # Get expected models: the model itself and all its descendants that are models or sources
                        expected_nodes = {model_id}
    
                        # Find child nodes
                        child_nodes = set()
                        for child in info["children"]:
                            if child in model_info["models"]:
                                child_nodes.add(child)
                                # Find grandchild nodes
                                for grandchild in model_info["models"][child]["children"]:
                                    if grandchild in model_info["models"] or grandchild in model_info["sources"]:
                                        child_nodes.add(grandchild)
                            elif child in model_info["sources"]:
                                child_nodes.add(child)
                                # Find grandchild nodes
                                for grandchild in model_info["sources"][child]["children"]:
                                    if grandchild in model_info["models"] or grandchild in model_info["sources"]:
                                        child_nodes.add(grandchild)
    
                        expected_nodes.update(child_nodes)
    
                        # Verify the selected nodes include the expected ones
                        assert set(extractor.selected_models).issuperset(expected_nodes)
                        return
        
        # Skip test if no suitable model found
        pytest.skip("No model with both child and grandchild found in test data")

    def test_filtered_descendants(self, extractor, model_info):
        """Test getting descendants filtered by another selector."""
        # Find a model with descendants and a common tag among them
        tag_counts = {}
        model_tags = {}
        
        # Count tag occurrences across models
        for node_id, info in model_info["models"].items():
            for tag in info.get("tags", []):
                if tag not in tag_counts:
                    tag_counts[tag] = 0
                    model_tags[tag] = []
                tag_counts[tag] += 1
                model_tags[tag].append(node_id)
        
        # Find a common tag (used by multiple models)
        common_tags = [tag for tag, count in tag_counts.items() if count > 1]
        
        if common_tags:
            tag = common_tags[0]
            tagged_nodes = model_tags[tag]
            
            # Find a model with descendants
            for node_id in tagged_nodes:
                if model_info["models"][node_id]["children"]:
                    node_name = model_info["models"][node_id]["name"]
                    
                    # Get nodes that are both descendants of this node and have the tag
                    selector = f"{node_name}+,tag:{tag}"
                    extractor.selected_models = extractor._parse_selectors([selector])
                    
                    # Verify results - should include nodes with tag that are descendants
                    for selected in extractor.selected_models:
                        # Should have the tag
                        assert tag in model_info["models"][selected]["tags"]
                        
                        # Should test only one case
                        return
        
        # Skip test if no suitable model found
        pytest.skip("No model with tagged descendants found in test data")

    def test_nested_union_and_intersection(self, extractor, model_info):
        """Test complex nested union and intersection operators."""
        # This test is complex and requires specific data patterns
        # We'll make a simpler version based on the available test data
        
        # Find two tags
        all_tags = set()
        for info in model_info["models"].values():
            all_tags.update(info.get("tags", []))
        
        if len(all_tags) >= 2:
            tags = list(all_tags)[:2]
            
            # Create a complex selector: models with tag1 OR (models with tag2 AND ancestors of some model)
            # Find a model with ancestors first
            for model_id, info in model_info["models"].items():
                if info["parents"]:
                    model_name = info["name"]
                    selector = f"tag:{tags[0]} tag:{tags[1]},+{model_name}"
                    
                    # Run the selection
                    extractor.selected_models = extractor._parse_selectors([selector])
                    
                    # Verify we got some results
                    assert len(extractor.selected_models) > 0
                    
                    # We can't easily predict the exact expected nodes, but we can verify 
                    # that each node has either tag1 or both tag2 and is an ancestor of our node
                    for selected in extractor.selected_models:
                        has_tag1 = tags[0] in model_info["models"][selected].get("tags", [])
                        has_tag2 = tags[1] in model_info["models"][selected].get("tags", [])
                        is_ancestor_or_self = selected == model_id or selected in info["parents"]
                        
                        assert has_tag1 or (has_tag2 and is_ancestor_or_self)
                    
                    # Only test one case
                    return
        
        # Skip test if no suitable model found
        pytest.skip("Test data doesn't have the required structure for complex selection") 