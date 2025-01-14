from __future__ import annotations

from starlette.responses import Response

from src.app_config import app
from src.check_return_type import check_response
from src.dependencies import (
    UnpackedProvisioningRequestDep,
    UnpackedUnprovisioningRequestDep,
    UnpackedUpdateAclRequestDep,
    FabricServiceDep,
    AzureFabricServiceDep,
    SQLSchemaMapperDep
)
from src.models.api_models import (
    ProvisioningStatus,
    SystemErr,
    ValidationError,
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
    Status1
)
from src.utility.logger import get_logger
from src.models.data_product_descriptor import FabricOutputPort, SinkKind

logger = get_logger()


@app.post(
    "/v1/provision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def provision(request: UnpackedProvisioningRequestDep,fabricService: FabricServiceDep, schemaService: SQLSchemaMapperDep, azureServiceapi: AzureFabricServiceDep) -> Response:
    """
    Deploy a data product or a single component starting from a provisioning descriptor
    """

    if isinstance(request, ValidationError):
        return check_response(out_response=request)
    
# Var to SnakeCase
    data_product, component_id = request
    logger.info("Provisioning component with id: " + component_id)
    componentToProvision = data_product.get_typed_component_by_id(component_id, FabricOutputPort)
    dc_table_name = componentToProvision.specific.table
    dc_schema_ = componentToProvision.dataContract.schema_
    sink = componentToProvision.specific.sink
    file_path = componentToProvision.specific.file_path
    dev_group = "group:" + data_product.devGroup
    if sink == SinkKind.DWH:
        sql_schema = schemaService.generate_sql_schema(schema = dc_schema_, nullable = True)
        try:
            fabricService.get_sql_endpoint(workspace=componentToProvision.specific.workspace, dwh=componentToProvision.specific.warehouse)        
            if fabricService.create_table(table_name=dc_table_name, schema=sql_schema) == True:
                fabricService.apply_acl_to_dwh_table(azureServiceapi.update_acl([dev_group]),dc_table_name,True)
                resp = ProvisioningStatus(status=Status1.COMPLETED,result="Provisioning completed")
            else:
                resp = ProvisioningStatus(status=Status1.FAILED,result="Provisioning not completed")
        except Exception as e:
            resp = SystemErr(error=f'Provisioning not completed, the error is: {e}' )
        return check_response(out_response=resp)
    elif sink == SinkKind.LAKEHOUSE:
        try:
            # if fabricService.load_table(workspace_id=componentToProvision.specific.workspace, 
            #                             relative_path=file_path, 
            #                             lakehouse_id=componentToProvision.specific.warehouse, 
            #                             table_name=dc_table_name,
            #                             file_format= componentToProvision.specific.fileFormat):
            if 1 == 1:
               fabricService.apply_acl_to_lakehouse_table(componentToProvision.specific.workspace,componentToProvision.specific.warehouse, azureServiceapi.get_user_id_lk('lorenzo.pirazzini@agilelab.it'),dc_table_name,True)
               resp = ProvisioningStatus(status=Status1.COMPLETED,result="Provisioning completed" )
            else:
                resp = ProvisioningStatus(status=Status1.FAILED,result="Provisioning not completed") 
        except Exception as e:
            resp = SystemErr(error=f'Provisioning not completed, the error is: {e}' )

        return check_response(out_response=resp)


@app.get(
    "/v1/provision/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_status(token: str) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/unprovision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def unprovision(request: UnpackedUnprovisioningRequestDep, fabricService: FabricServiceDep) -> Response:
    """
    Undeploy a data product or a single component
    given the provisioning descriptor relative to the latest complete provisioning request
    """  # noqa: E501

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, component_id, remove_data = request

    logger.info("Unprovisioning component with id: " + component_id)
    componentToUnprovision = data_product.get_typed_component_by_id(component_id, FabricOutputPort)
    fabricService.get_sql_endpoint(componentToUnprovision.specific.workspace,componentToUnprovision.specific.warehouse)
    try:
        if fabricService.drop_table(componentToUnprovision.specific.table) == True:
            resp = ProvisioningStatus(status=Status1.COMPLETED,result="Unprovisioning completed") 
        else:
            resp = ProvisioningStatus(status=Status1.FAILED, result="Unprovisioning not completed")
    except Exception as e:
        resp = SystemErr(error=f'Response {e}' )
    return check_response(out_response=resp)




@app.post(
    "/v1/updateacl",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def updateacl(request: UnpackedUpdateAclRequestDep, fabricService: FabricServiceDep, azureServiceapi: AzureFabricServiceDep) -> Response:
    """
    Request the access to a specific provisioner component
    """

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, component_id, witboost_users = request
    componentToProvision = data_product.get_typed_component_by_id(component_id, FabricOutputPort)
    fabricService.get_sql_endpoint(componentToProvision.specific.workspace,componentToProvision.specific.warehouse)

    try:
        if fabricService.apply_acl_to_dwh_table(acl_entries=azureServiceapi.update_acl(witboost_users),table_name= componentToProvision.specific.table) == True:
            resp = ProvisioningStatus(status=Status1.COMPLETED,result="Acl updated")
        else:
            resp = ProvisioningStatus(status=Status1.FAILED, result="Acl not updated")
    except Exception as err:
        resp = SystemErr(error=f"Error{err}")

    return check_response(out_response=resp)


@app.post(
    "/v1/validate",
    response_model=None,
    responses={"200": {"model": ValidationResult}, "500": {"model": SystemErr}},
    tags=["SpecificProvisioner"],
)
def validate(request: UnpackedProvisioningRequestDep) -> Response:
    """
    Validate a provisioning request
    """

    if isinstance(request, ValidationError):
        return check_response(ValidationResult(valid=False, error=request))

    data_product, component_id = request

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v2/validate",
    response_model=None,
    responses={
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def async_validate(
    body: ValidationRequest,
) -> Response:
    """
    Validate a deployment request
    """

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.get(
    "/v2/validate/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ValidationStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_validation_status(
    token: str,
) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)
