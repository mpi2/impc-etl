"""
Command line Interface for impc_etl package.
"""
import click
import os


@click.group()
def cli():
    pass


@click.command()
@click.argument('input_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('output_dir', type=click.Path(file_okay=False, dir_okay=True))
def generate_jpegs(input_dir, output_dir):
    """
    Generate JPEG images from an images directory which can have different sizes and file formats.
    For each image found on the input it should generate a JPEG image with a full resolution, and one image to be used as thumbnail.
    - Requires an input directory and an output directory.
    - Should allow to define suffixes for naming the full resolution and the thumbnail images to be generated.
    """
    if os.path.exists(output_dir):
        click.echo(f"Error: The output directory '{output_dir}' already exists.", err=True)
        raise SystemExit(1)
    
    os.mkdir(output_dir)

    click.echo(f"Generating list of input files for input directory {input_dir}")
    for root, dirs, files in os.walk(input_dir):
        for name in files:
            with open(os.path.join(output_dir, "input_files.txt"), "a", encoding="utf-8") as f:
                f.write(os.path.join(root, name) + "\n")

    click.echo(f"Generating JPEG files from {input_dir} to {output_dir}")


cli.add_command(generate_jpegs)

if __name__ == "__main__":
    cli()
