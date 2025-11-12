import pandas as pd

from src.rest.client import OrikTabularClient
from src.rest.models import ValidateMappingRequest, UploadDataRequest


def main():
    client = OrikTabularClient(base_url="https://app.alphos-services.com/api/v1")

    mapping = {
        "explode": {
            "path": "annotations",
            "emit_root_when_empty": False
        },
        "columns": {
            "image_id": { "path": "image.id" },
            "image_url": { "path": "image.url" },

            "x": { "rel_path": "bbox.x", "cast": "float" },
            "y": { "rel_path": "bbox.y", "cast": "float" },
            "w": { "rel_path": "bbox.w", "cast": "float" },
            "h": { "rel_path": "bbox.h", "cast": "float" },

            "labels_joined": { "join": { "over": {"rel_path": "labels"}, "sep": "|" } },
            "first_label": { "index": { "of": {"rel_path": "labels"}, "at": 0 } },

            "confidence": { "rel_path": "confidence", "cast": "float", "default": 1.0 }
        }
    }

    sample_data = {
        "image": {"id": "img-001", "url": "https://cdn.example/1.jpg"},
        "annotations": [
            {"bbox": {"x": 10, "y": 20, "w": 100, "h": 80}, "labels": ["person"], "confidence": 0.98},
            {"bbox": {"x": 200, "y": 35, "w": 60, "h": 60}, "labels": ["dog", "pet"], "confidence": 0.91}
        ]
    }

    request = ValidateMappingRequest(
        mapping=mapping,
        sample_data=sample_data,
        sample_is_batched=False
    )

    try:
        result = client.validate_mapping(
            request=request
        )
        if not result.is_valid:
            for error in result.errors:
                print(f"Error: {error}")

            exit(0)
        else:
            print(f"Validation result: {result.is_valid}")

        if request.sample_data is not None and result.sample_is_valid:
            print("Sample data is valid according to the mapping.")
            result_df = pd.DataFrame(result.sample_result)
            print(result_df.to_string())
        else:
            print("Sample data is NOT valid according to the mapping / no sample data provided.")

        upload_request = UploadDataRequest(
            auth_token="",     # Your API token here,
            mapping_uuid="",   # Your mapping UUID here
            data=sample_data,  # or a list of data records
            is_batched=False
        )

        upload_response = client.upload(
            request=upload_request
        )
        if upload_response.success:
            print(upload_request.model_dump_json(indent=2))
            print(f"Upload successful! Processed records: {upload_response.processed_records}")
        else:
            print(f"Upload failed: {upload_response.message}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
