"""Dispatch model-specific output checks without changing native model creation."""

from multi_task import task_contract
import multi_task
import rocket_launching_contract


def export_contract(config):
    """Resolve public outputs for models that require a manifest contract."""
    return rocket_launching_contract.rocket_contract(config) or task_contract(config)


def validate_predictions(predictions, contract, batch_size):
    """Check each supported model's public prediction values."""
    if contract.get("architecture") == "rocket_launching":
        rocket_launching_contract.validate_predictions(predictions, batch_size)
    else:
        multi_task.validate_predictions(predictions, contract, batch_size)


def validate_scripted_export(directory, contract):
    """Run the exported model before publishing its output contract."""
    if contract.get("architecture") == "rocket_launching":
        rocket_launching_contract.validate_scripted_export(directory, contract)
    else:
        multi_task.validate_scripted_export(directory, contract)
