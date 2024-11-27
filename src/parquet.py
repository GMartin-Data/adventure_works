import csv
import io
import logging
import os

from PIL import Image
import polars as pl
from tqdm import tqdm

# Configure logger
logging.basicConfig(
    filename="./logs/parquet_processing.log",
    level=logging.DEBUG,
    format="%(name)s - %(asctime)s - %(levelname)s - %(message)s",
)

# Logger instanciation
logging.getLogger(__name__)

# Set files and folder
images_folder = "data/parquet/parquet_images/"
csv_file = "data/parquet/parquet_metadata.csv"
os.makedirs(images_folder, exist_ok=True)

# Open the CSV file for writing
with open(csv_file, mode="w", newline="", encoding="utf-8") as csv_writefile:
    csv_writer = csv.writer(csv_writefile)
    csv_fields_written = False  # Flag to ensure CSV fields are only written once

    # Iterate over all Parquet files
    for file in os.listdir("data/product_eval"):
        lazy_df = pl.scan_parquet(f"data/product_eval/{file}")

        # Collect the schema once, then the data
        if not csv_fields_written:
            csv_fields = lazy_df.collect_schema().names()[1:]
            csv_writer.writerow(csv_fields)  # Writing CSV file header
            csv_fields_written = True

        df = lazy_df.collect()
        total_rows = df.height

        # Process the DataFrame with a progress bar
        for row in tqdm(
            df.iter_rows(named=True), total=total_rows, desc=f"Processing: '{file}'"
        ):
            # Extract image name and write metadata on CSV
            image_name = row["item_ID"].split("-")[1]
            csv_writer.writerow(
                [
                    image_name,
                    row["query"],
                    row["title"],
                    row["position"],
                ]
            )
            # Decode and save image as PNG
            try:
                img = row["image"]["bytes"]
                with Image.open(io.BytesIO(img)) as image:
                    image.save(
                        os.path.join(images_folder, f"{image_name}.png"), format="PNG"
                    )
            except Exception as e:
                logging.error(f"❌ Error processing image '{image_name}':\n{e}")
        logging.info(f"✅ Successfully processed file: '{file}'")
