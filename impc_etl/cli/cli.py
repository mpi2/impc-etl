"""
Command line Interface for impc_etl package.
"""
import click


@click.group()
def cli():
    pass


@click.command()
def generate_jpegs():
    """
    Generate JPEG images from an images directory which can have different sizes and file formats.
    For each image found on the input it should generate a JPEG image with a full resolution, and one image to be used as thumbnail.
    - Should allow to define an input and output directory. By default, the output location should be the same as the input directory.
    - Should allow to define suffixes for naming the full resolution and the thumbnail images to be generated.
    """
    click.echo("Generated all JPEG files")
