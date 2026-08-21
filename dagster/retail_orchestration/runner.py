import shlex
import subprocess
from collections.abc import Sequence
from pathlib import Path

from dagster import AssetExecutionContext, Failure, MetadataValue


def run_cli_command(
    context: AssetExecutionContext,
    command: Sequence[str],
    *,
    working_directory: Path,
) -> None:
    """Run a CLI command and stream its output into the Dagster run logs."""

    display_command = shlex.join(command)

    context.log.info("Starting command: %s", display_command)
    context.log.info("working directory: %s", working_directory)

    try:
        process = subprocess.Popen(
            list(command),
            cwd=working_directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError as error:
        raise Failure(
            description=f"Unable to start command: {display_command}",
            metadata={
                "command": MetadataValue.text(display_command),
                "working_directory": MetadataValue.path(working_directory),
                "error": MetadataValue.text(str(error)),
            },
        ) from error

    if process.stdout is None:
        process.kill()

        raise Failure(
            description=f"Unable to capture output from command: {display_command}",
            metadata={
                "command": MetadataValue.text(display_command),
            },
        )

    for output_line in process.stdout:
        context.log.info(output_line.rstrip())

    exit_code = process.wait()

    if exit_code != 0:
        raise Failure(
            description=f"Command failed with exit code {exit_code}",
            metadata={
                "command": MetadataValue.text(display_command),
                "working_directory": MetadataValue.path(working_directory),
                "exit_code": exit_code,
            },
        )

    context.log.info("Command completed successfully: %s", display_command)
