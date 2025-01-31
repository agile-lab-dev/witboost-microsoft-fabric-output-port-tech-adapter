from pathlib import Path

from starlette.testclient import TestClient

from src.main import app
from src.models.api_models import (
    DescriptorKind,
    ProvisioningRequest,
)

client = TestClient(app)


def test_validate_invalid_descriptor():
    validate_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor"
    )

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert "Unable to parse the descriptor." in resp.json().get("error").get("errors")


def test_validate_valid_descriptor():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()

    validate_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str
    )

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert resp.json() == {"error": None, "valid": True}
