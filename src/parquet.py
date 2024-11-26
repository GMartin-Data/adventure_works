import base64
import csv
import io
import os

from PIL import Image
import polars as pl
from rich import print as rprint


for file in os.listdir("data/product_eval"):
    lazy_df = pl.scan_parquet(f"data/product_eval/{file}")

    csv_fields = lazy_df.collect_schema().names()[1:]

    images_folder = "data/parquet/parquet_images/"
    csv_file = "data/parquet/parquet_metadata.csv"

    # Create output folders if not exists
    os.makedirs(images_folder, exist_ok=True)

    # Manage metadata in CSV file
    with open(csv_file, mode="w", newline="", encoding="utf-8") as csv_writefile:
        csv_writer = csv.writer(csv_writefile)
        csv_writer.writerow(csv_fields)  # Writing CSV file header

        # Process the lazy DataFrame
        for idx, row in enumerate(lazy_df.collect().iter_rows(named=True)):
            # Write metadata on CSV
            image_name = row["image"]["path"].split(".")[0]
            csv_writer.writerow(
                [
                    row["item_ID"],
                    row["query"],
                    row["title"],
                    row["position"],
                    image_name
                ]
            )
            # Decode and save image as PNG
            try:
                img = row["image"]["bytes"]
                with Image.open(io.BytesIO(img)) as image:
                    image.save(os.path.join(images_folder, f"{image_name}.png"), format="PNG")
            except Exception as e:
                print(f"Error processing image at index: {idx}:\n{e}")
