"""
Jobs to generate JPEG images for all the IMPC Imaging data.
- Should be able to support different input formats
- Should have a function to generate a full resolution JPEG image
- Should have a function to generate a thumbnail image
"""

import click

@click.command()
@click.option("--manifest", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--batch-from", type=int, required=True)
@click.option("--batch-to", type=int, required=True)
@click.option("--full-suffix", type=str, required=True, help="Full size image suffix")
@click.option("--thumbnail-suffix", type=str, required=True, help="Thumbnail suffix")
@click.option("--thumbnail-width", type=int, required=True, help="Width of thumbnail")
@click.option("--thumbnail-quality", type=int, required=True, help="Quality of thumbnail")
def process_images(manifest, batch_from, batch_to, full_suffix, thumbnail_suffix, thumbnail_width, thumbnail_quality):
    """Processes images in the given manifest within the specified batch range.

    MANIFEST: Path to the manifest file.
    BATCH_FROM: Starting batch number.
    BATCH_TO: Ending batch number.
    """
    file_names = open(manifest, "r").readlines()[batch_from:batch_to]
    file_names = [x.strip().split("\t") for x in file_names]
    print(file_names)
    click.echo(f"Manifest file: {manifest}")
    click.echo(f"Processing batches from {batch_from} to {batch_to}")
    click.echo(f"Full size suffix: {full_suffix}")
    click.echo(f"Thumbnail suffix: {thumbnail_suffix}")
    click.echo(f"Thumbnail width: {thumbnail_width}")
    click.echo(f"Thumbnail quality: {thumbnail_quality}")

    # Placeholder for actual processing logic.
    click.echo("Processing images... (this is a placeholder)")

if __name__ == "__main__":
    process_images()
