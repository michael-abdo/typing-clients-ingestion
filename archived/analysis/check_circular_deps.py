#!/usr/bin/env python3
"""Check for circular dependencies in the utils directory"""

import ast
import os
from pathlib import Path
from collections import defaultdict, deque

def get_imports(filepath):
    """Extract all imports from a Python file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"Warning: Could not parse {filepath}: {e}")
        return []
    
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith('utils.') or not '.' in alias.name:
                    imports.append(alias.name.replace('utils.', ''))
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                if node.module.startswith('utils.'):
                    imports.append(node.module.replace('utils.', ''))
                elif node.module.startswith('.') and len(node.module) > 1:
                    # Relative import like .config
                    imports.append(node.module[1:])
    
    return imports

def find_circular_dependencies(import_map):
    """Find circular dependencies using DFS"""
    def has_cycle(node, visited, rec_stack, path):
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        
        for neighbor in import_map.get(node, []):
            if neighbor in import_map:  # Only check modules we know about
                if neighbor not in visited:
                    if has_cycle(neighbor, visited, rec_stack, path):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    cycles.append(cycle)
                    return True
        
        path.pop()
        rec_stack.remove(node)
        return False
    
    cycles = []
    visited = set()
    
    for module in import_map:
        if module not in visited:
            has_cycle(module, visited, set(), [])
    
    return cycles

def main():
    """Main dependency checker"""
    print("="*60)
    print("Circular Dependency Checker")
    print("="*60)
    
    # Check utils directory
    utils_dir = Path('utils')
    if not utils_dir.exists():
        print("Error: utils directory not found")
        return 1
    
    import_map = {}
    module_files = {}
    
    # Scan all Python files in utils
    for py_file in utils_dir.glob('*.py'):
        if py_file.name == '__init__.py':
            continue
            
        module_name = py_file.stem
        imports = get_imports(py_file)
        
        # Filter to only utils modules
        utils_imports = []
        for imp in imports:
            if imp in ['config', 'patterns', 'logging_config', 'error_handling', 
                      'csv_manager', 'sanitization', 'validation', 'retry_utils',
                      'monitoring', 'comprehensive_file_mapper', 'path_setup',
                      'extract_links', 'download_drive', 'download_youtube']:
                utils_imports.append(imp)
        
        import_map[module_name] = utils_imports
        module_files[module_name] = str(py_file)
        print(f"Module: {module_name} imports: {utils_imports}")
    
    print(f"\nAnalyzing {len(import_map)} modules...")
    
    # Find circular dependencies
    cycles = find_circular_dependencies(import_map)
    
    if cycles:
        print(f"\n❌ Found {len(cycles)} circular dependencies:")
        for i, cycle in enumerate(cycles, 1):
            print(f"\nCycle {i}: {' -> '.join(cycle)}")
        return 1
    else:
        print("\n✅ No circular dependencies found!")
        
    # Show dependency tree
    print(f"\nDependency summary:")
    for module, deps in import_map.items():
        if deps:
            print(f"  {module} depends on: {', '.join(deps)}")
        else:
            print(f"  {module} has no internal dependencies")
    
    return 0

if __name__ == "__main__":
    exit(main())