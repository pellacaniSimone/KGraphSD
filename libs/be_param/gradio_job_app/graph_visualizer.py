"""
Graph Visualization Module
=========================

This module provides functionality to visualize and interact with Apache AGE graphs
using NetworkX and Matplotlib. It allows loading graph data from the database and
visualizing it as a directed graph with Gradio integration.
"""

import networkx as nx
import matplotlib.pyplot as plt
import psycopg2
import ast
import io
from PIL import Image
import base64
from typing import List, Tuple, Dict, Optional, Any, Union


class GraphVisualizer:
    """
    Class for visualizing Apache AGE graphs using NetworkX with Gradio integration.
    
    This class provides methods to:
    - Load graph data from Apache AGE
    - Convert it to NetworkX format
    - Visualize the graph with Matplotlib
    - Return the visualization as a PIL Image for Gradio or as base64-encoded string
    """
    
    def __init__(self, db_config: Dict[str, Any], graph_name: str, debug: bool = False):
        """
        Initialize the graph visualizer.
        
        Args:
            db_config: Database connection parameters
            graph_name: Name of the graph in Apache AGE
            debug: Whether to print debug information
        """
        self.db_config = db_config
        self.graph_name = graph_name
        self.debug = debug
    
    def get_graph_data(self) -> Tuple[List[str], List[Tuple[str, str, Dict[str, str]]]]:
        """
        Retrieves nodes and relationships from Apache AGE.
        
        Returns:
            Tuple of (nodes, edges) where nodes is a list of node IDs and
            edges is a list of (source, target, attributes) tuples
        """
        if self.debug:
            print("[DEBUG] Starting graph data retrieval")
        try:
            conn = psycopg2.connect(**self.db_config)
            if self.debug:
                print(f"[DEBUG] Connected to database: {self.db_config['dbname']}")
            cur = conn.cursor()
            
            # Query to get all nodes
            if self.debug:
                print(f"[DEBUG] Executing node query for graph: {self.graph_name}")
            cur.execute(f"""
                CREATE EXTENSION IF NOT EXISTS age;
                LOAD 'age';
                SET search_path = ag_catalog, "$user", public;
                SELECT * FROM cypher('{self.graph_name}', $$ MATCH (n) RETURN n $$) AS (node agtype);
            """)
            nodes = []
            for record in cur.fetchall():
                json_str = record[0].split('::vertex')[0]
                dictionary = ast.literal_eval(json_str)
                nodes.append(dictionary['properties']['entity'])
            
            if self.debug:
                print(f"[DEBUG] Retrieved {len(nodes)} nodes")

            # Query to get all relationships
            if self.debug:
                print(f"[DEBUG] Executing relationship query for graph: {self.graph_name}")
            cur.execute(f"""
                CREATE EXTENSION IF NOT EXISTS age;
                LOAD 'age';
                SET search_path = ag_catalog, "$user", public;
                SELECT * FROM cypher('{self.graph_name}', $$ MATCH (n)-[r]->(m) RETURN n, r, m $$) AS (node1 agtype, rel agtype, node2 agtype);
            """)
            edges = []
            
            for record in cur.fetchall():
                # Extract source node
                source_json = record[0].split('::vertex')[0]
                source_dict = ast.literal_eval(source_json)
                source_id = source_dict['properties']['entity']
                
                # Extract target node
                target_json = record[2].split('::vertex')[0]
                target_dict = ast.literal_eval(target_json)
                target_id = target_dict['properties']['entity']
                
                # Extract relationship type
                rel_json = record[1].split('::edge')[0]
                rel_dict = ast.literal_eval(rel_json)
                rel_type = rel_dict['properties']["label"]
                edges.append((source_id, target_id, {"label": rel_type}))
            
            if self.debug:
                print(f"[DEBUG] Retrieved {len(edges)} relationships")
            return nodes, edges
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Error in get_graph_data: {e}")
            return [], []
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()
            if self.debug:
                print("[DEBUG] Database connection closed")

    def plot_graph(self) -> Image.Image:
        """
        Plots the graph with labels only on edges and returns a PIL Image.
        
        Returns:
            PIL.Image: The rendered graph visualization as an image
        """
        if self.debug:
            print("[DEBUG] Starting graph plotting")
        try:
            nodes, edges = self.get_graph_data()
            
            if not nodes or not edges:
                if self.debug:
                    print("[DEBUG] No graph data available for plotting")
                # Create a simple image with error message if no data
                fig, ax = plt.subplots(figsize=(8, 6))
                ax.text(0.5, 0.5, "No graph data available", 
                        horizontalalignment='center', verticalalignment='center')
                ax.axis('off')
            else:
                if self.debug:
                    print(f"[DEBUG] Creating graph with {len(nodes)} nodes and {len(edges)} edges")
                G = nx.DiGraph()
                G.add_nodes_from(nodes)
                G.add_edges_from([(src, tgt, attrs) for src, tgt, attrs in edges])  # Include attributes

                # Improved layout
                if self.debug:
                    print("[DEBUG] Calculating graph layout")
                pos = nx.spring_layout(G, seed=42)

                fig, ax = plt.subplots(figsize=(10, 8))
                nx.draw(G, pos, with_labels=True, node_color='lightblue', edge_color='black', 
                        node_size=2000, font_size=12, font_weight='bold', arrows=True, ax=ax)
                
                # Add labels to edges
                if self.debug:
                    print("[DEBUG] Adding edge labels")
                edge_labels = nx.get_edge_attributes(G, 'label')
                nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red')
                
                plt.title(f"Graph from Apache AGE: {self.graph_name}")
            
            # Convert plot to image
            if self.debug:
                print("[DEBUG] Converting plot to image")
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close(fig)
            return Image.open(buf)
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Error in plot_graph: {e}")
            # Return a simple error image
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.text(0.5, 0.5, f"Error plotting graph: {str(e)}", 
                    horizontalalignment='center', verticalalignment='center')
            ax.axis('off')
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close(fig)
            return Image.open(buf)
    
