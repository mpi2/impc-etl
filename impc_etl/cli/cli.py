"""
Command line Interface for impc_etl package.
"""
import click
import os

from pathlib import Path


@click.group()
def cli():
    pass


@click.command()
@click.argument('input_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('output_dir', type=click.Path(file_okay=False, dir_okay=True))
@click.option('--batch-size', default=1000, type=int, help='Batch size for processing (default: 1000)')
def generate_jpegs(input_dir, output_dir, batch_size):
    """Generate JPEG images from an images directory which can have different sizes and file formats.
    For each image found on the input it should generate a JPEG image with a full resolution, and one image to be used as thumbnail.
    - Requires an input directory and an output directory.
    - Should allow to define suffixes for naming the full resolution and the thumbnail images to be generated.
    """
    if os.path.exists(output_dir):
        click.echo(f"Error: The output directory '{output_dir}' already exists.", err=True)
        raise SystemExit(1)
    
    os.mkdir(output_dir)

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    total_files = 0

    click.echo(f"Generating list of input files for input directory {input_dir}")
    with open(os.path.join(output_dir, "manifest.tsv"), "w", encoding="utf-8") as f:
        for input_path in input_dir.rglob("*"):
            if input_path.is_file():
                output_path = output_dir / input_path.relative_to(input_dir)
                f.write(f"{input_path.resolve()}\t{output_path.resolve()}\n")
                total_files += 1

    click.echo(f"Generating JPEG files from {input_dir} to {output_dir}")


cli.add_command(generate_jpegs)

if __name__ == "__main__":
    cli()
