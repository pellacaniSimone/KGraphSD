"""
Gradio web interface for job catalog application.
This module provides a user interface for submitting job offers to the catalog
and visualizing the knowledge graph.
"""
import os
from typing import Dict, Any, List, Union, Optional

import gradio as gr

# Configure debug mode
DEBUG = True
if DEBUG:
  os.environ['PROD'] = 'False'  # permit production on different instance

# Import application backend
from libs.be_param.gradio_job_app.gradio_BE_app import JobCatalogApp
from libs.be_param.gradio_job_app.graph_visualizer import GraphVisualizer

# Graph configuration
GRAPH_NAME = "test_schema"  # Use the same graph name as in the example

# Database configuration with fallback mechanism
# Try to use the configuration from the working example
DB_CONFIG = {
  "dbname": "db_multimodale_multistruttura", 
  "user": "postgres",
  "password": "", # password here
  "host": "debianMultiModelDB.local.lan",
  "port": "5432",
}


def create_interface() -> gr.Blocks:
  """
  Create and configure the Gradio interface for the job catalog application.
  
  Returns:
    gr.Blocks: Configured Gradio interface
  """
  with gr.Blocks(title="Job Catalog App") as demo:
    gr.Markdown("# Job Catalog Application")
    
    with gr.Tabs():
      with gr.TabItem("Add Job Offer"):
        if DEBUG:
          print("[DEBUG] Setting up Add Job Offer tab")
        with gr.Row():
          with gr.Column():
            link = gr.Textbox(label="Link to Job Offer")
            job_platform_input = gr.Textbox(label="Job Platform", value="LinkedIn")
            language_radio = gr.Radio(["ita", "eng"], label="Language", value="ita")
            
          with gr.Column():
            offer_title = gr.Textbox(label="Job Title")
            client_name = gr.Textbox(label="Company Name")
            position = gr.Textbox(label="Location")
            presence = gr.Radio( ["presenza", "remoto", "ibrido"],
              label="Work Mode",  value="ibrido"
            )
        
        with gr.Row():
          with gr.Column():
            relevant = gr.Textbox(label="Relevant Information", lines=5)
            full_input = gr.Textbox(label="Full Job Description", lines=10)
        
        with gr.Row():
          submit_button = gr.Button("Submit Job Offer")
          clear_button = gr.Button("Clear Form")
        
        output = gr.JSON(label="Result")
        
        # Initialize application backend
        app = JobCatalogApp()
        
        # Connect the submit button to the backend processing function
        submit_button.click(
          app.process_input,
          inputs=[ link, job_platform_input, 
            language_radio, offer_title, client_name, 
            position, presence,  relevant, full_input
          ],  
          outputs=output
        )
        
        # Clear form function
        def clear_inputs():
          return [""] * 8
        
        clear_button.click( fn=clear_inputs, inputs=[],
          outputs=[
                link, job_platform_input, offer_title, client_name, 
                position, presence, relevant, full_input
            ]
        )
        
      with gr.TabItem("View Knowledge Graph"):
        if DEBUG:
          print("[DEBUG] Setting up View Knowledge Graph tab")
        gr.Markdown("## Knowledge Graph Visualization")
        
        refresh_button = gr.Button("Refresh Graph")
        graph_output = gr.Image(label="Knowledge Graph")
        
        # Initialize graph visualizer with database configuration
        visualizer = GraphVisualizer(DB_CONFIG, GRAPH_NAME, debug=DEBUG)
        
        # Connect refresh button to graph visualization
        refresh_button.click( visualizer.plot_graph,
          inputs=[], outputs=graph_output
        )
  
  return demo


if __name__ == "__main__":
  # Create and launch the interface
  interface = create_interface()
  interface.launch()
