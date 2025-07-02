"""
Command module for managing descriptions in dbt projects.
"""

import click
from rich.console import Console

console = Console()

@click.group()
def descriptions() -> None:
    """Commands for managing dbt model descriptions."""
    pass


@descriptions.command()
@click.option('--project', '-p', required=True, help='Specify the dbt project by name')
@click.option('--model', '-m', help='Specific model name to process (optional)')
@click.option('--expand', is_flag=True, help='Expand existing descriptions if any')
@click.option('--provider', '-pr', type=click.Choice(['openai', 'anthropic'], case_sensitive=False), default='openai', help='LLM provider to use')
@click.option('--dry-run', is_flag=True, help='Show generated descriptions without writing to files')
@click.option('--interactive', '-i', is_flag=True, help='Ask for confirmation before updating each model')
def generate(project, model, expand, provider, dry_run, interactive):
    """Generate or expand descriptions for dbt models using LLMs."""
    from ..core.project_discovery import ProjectDiscovery
    from ..core.model_parser import ModelParser
    from ..integrations.llm import LLMDescriptionGenerator, LLMProvider, ModelContext
    
    discovery = ProjectDiscovery()
    dbt_project = discovery.get_project_by_name(project)

    if not dbt_project:
        console.print(f"[red]Project {project} not found[/red]")
        return

    try:
        generator = LLMDescriptionGenerator(LLMProvider(provider))
    except ValueError as e:
        console.print(f"[red]Error setting up LLM provider: {e}[/red]")
        console.print("[yellow]Make sure to set the appropriate environment variable (OPENAI_API_KEY or ANTHROPIC_API_KEY)[/yellow]")
        return

    parser = ModelParser(dbt_project.path)
    models_to_process = discovery.list_models_in_project(project)
    
    # Filter to specific model if requested
    if model:
        models_to_process = [m for m in models_to_process if m.stem == model]
        if not models_to_process:
            console.print(f"[red]Model '{model}' not found in project '{project}'[/red]")
            return

    console.print(f"[bold blue]Processing {len(models_to_process)} model(s) in project '{project}'[/bold blue]\n")
    
    for model_path in models_to_process:
        console.print(f"[bold cyan]📋 Processing: {model_path.name}[/bold cyan]")
        
        # Parse the model to get existing schema information
        model_info = parser.parse_model(model_path)
        
        # Create context for LLM
        context = ModelContext(
            name=model_info.name,
            sql_content=model_info.sql_content,
            existing_description=model_info.existing_description,
            columns=model_info.columns,
            project_name=dbt_project.name
        )

        try:
            # Generate descriptions
            with console.status(f"[bold green]Generating descriptions for {model_info.name}..."):
                descriptions = generator.generate_descriptions(context, expand_existing=expand)

            # Display results
            console.print(f"[bold green]✅ Generated descriptions for {model_info.name}[/bold green]")
            console.print(f"[bold]Model Description:[/bold]")
            console.print(f"  {descriptions.model_description}\n")
            
            if descriptions.column_descriptions:
                console.print(f"[bold]Column Descriptions:[/bold]")
                for col, desc in descriptions.column_descriptions.items():
                    console.print(f"  [blue]{col}[/blue]: {desc}")
                console.print()
            
            # Handle file writing
            if not dry_run:
                should_update = True
                
                if interactive:
                    should_update = click.confirm(f"Update descriptions for {model_info.name}?")
                
                if should_update:
                    success = parser.update_model_descriptions(
                        model_info,
                        descriptions.model_description,
                        descriptions.column_descriptions
                    )
                    
                    if success:
                        console.print(f"[green]✅ Updated descriptions for {model_info.name}[/green]\n")
                    else:
                        console.print(f"[red]❌ Failed to update descriptions for {model_info.name}[/red]\n")
                else:
                    console.print(f"[yellow]⏭️  Skipped updating {model_info.name}[/yellow]\n")
            else:
                console.print(f"[yellow]🔍 Dry run - no files modified[/yellow]\n")
                
        except Exception as e:
            console.print(f"[red]❌ Error processing {model_info.name}: {e}[/red]\n")
            continue
    
    if dry_run:
        console.print("[bold yellow]Dry run completed. Use --no-dry-run to actually update files.[/bold yellow]")
    else:
        console.print("[bold green]Description generation completed![/bold green]")


# Register the command
descriptions.add_command(generate)
