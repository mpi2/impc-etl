"""
Jobs to generate JPEG images for all the IMPC Imaging data.
- Should be able to support different input formats
- Should have a function to generate a full resolution JPEG image
- Should have a function to generate a thumbnail image
"""

from pathlib import Path
import subprocess
import click


def convert_image(input_path: str, output_path: str, width: int = None, quality: int = None):
    """Converts an image using the system 'convert' command.

    Parameters:
        input_path (str): The path to the input image file.
        output_path (str): The path to save the output image.
        width (int, optional): The desired width of the output image. Aspect ratio is maintained.
        quality (int, optional): The quality level of the output image (1-100).

    Raises:
        ValueError: If both input_path and output_path are not provided.
        subprocess.CalledProcessError: If the 'convert' command fails.
    """
    if not input_path or not output_path:
        raise ValueError("Both input_path and output_path must be specified.")

    # Construct the command.
    command = ["magick", input_path]

    # Add width resizing if specified.
    if width:
        command.extend(["-resize", f"{width}x"])

    # Add quality setting if specified.
    if quality:
        command.extend(["-quality", str(quality)])

    # Add the output path.
    command.append(output_path)

    # Execute the command.
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to convert image. {e}")
        raise


@click.command()
@click.option("--manifest", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--batch-from", type=int, required=True)
@click.option("--batch-to", type=int, required=True)
@click.option("--full-suffix", type=str, required=True, help="Full size image suffix")
@click.option("--thumbnail-suffix", type=str, required=True, help="Thumbnail suffix")
@click.option("--thumbnail-width", type=int, required=True, help="Width of thumbnail")
@click.option("--thumbnail-quality", type=int, required=True, help="Quality of thumbnail")
def process_images(
    manifest: str,
    batch_from: int,
    batch_to: int,
    full_suffix: str,
    thumbnail_suffix: str,
    thumbnail_width: int,
    thumbnail_quality: int
) -> None:
    """Processes images in the given manifest within the specified batch range."""
    click.echo(f"Manifest file: {manifest}")
    click.echo(f"Processing batches from {batch_from} to {batch_to}")
    click.echo(f"Full size suffix: {full_suffix}")
    click.echo(f"Thumbnail suffix: {thumbnail_suffix}")
    click.echo(f"Thumbnail width: {thumbnail_width}")
    click.echo(f"Thumbnail quality: {thumbnail_quality}")

    file_names = open(manifest, "r").readlines()[batch_from:batch_to]
    file_names = [x.strip().split("\t") for x in file_names]

    click.echo("Processing images...")
    for input_file, output_file_basename in file_names:
        output_dir = Path(output_file_basename).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_file_basename + full_suffix + ".jpg"
        thumbnail_file = output_file_basename + thumbnail_suffix + ".jpg"
        convert_image(input_file, output_file, width=None, quality=100)
        convert_image(input_file, thumbnail_file, width=thumbnail_width, quality=thumbnail_quality)
        print(input_file, output_file_basename)

if __name__ == "__main__":
    process_images()
