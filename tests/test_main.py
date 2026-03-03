# pylint: disable=redefined-outer-name

import sys
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from cryri.config import CryConfig, ContainerConfig, CloudConfig
from cryri.job_manager import JobManager
from cryri.utils import (
    create_job_description
)
from tests.utils.mocks import mock_path_resolution, make_is_dir_mock, mock_env_vars

runner = CliRunner()


@pytest.fixture
@mock_path_resolution(force_is_dir=make_is_dir_mock())
def basic_config():
    return CryConfig(
        container=ContainerConfig(
            image="test-image:latest",
            command="python script.py",
            work_dir="/test/dir",
        ),
        cloud=CloudConfig(
            region="SR006",
            instance_type="cpu.small",
        )
    )


@pytest.fixture
def job_manager():
    return JobManager(region="SR006")


def test_container_config_defaults():
    config = ContainerConfig()
    assert config.image is None
    assert config.command is None
    assert config.environment is None
    assert config.work_dir is None
    assert config.run_from_copy is False
    assert config.cry_copy_dir is None


@mock_path_resolution(cwd="/mock/fake/dir")
def test_create_job_description_basic(basic_config):
    description = create_job_description(basic_config)
    assert description == "-test-dir"


@mock_path_resolution(cwd="/mock/fake/dir")
def test_create_job_description_with_team(basic_config):
    basic_config.container.environment = {"TEAM_NAME": "test-team"}
    description = create_job_description(basic_config)
    assert description == "-test-dir #test-team"


@mock_env_vars(
    HOME="/mock/fake_user", MY_HOME="~/sub_user",
    WANDB_API_KEY="8aead3118j2ej28e2jee",
)
@mock_path_resolution(cwd="/mock/fake_dir/", force_is_dir=make_is_dir_mock())
def test_container_config_expand_resolve_fields_validators():
    config = ContainerConfig(
        image="test-image:latest",
        command="python script.py",
        environment={
            "HF_HOME": "$HOME/.cache/huggingface",
            "WANDB_API_KEY": "$WANDB_API_KEY",
            "WANDB_PROJECT": "LoRa-TinyLlama",
            "TEAM_NAME": "look/like/path",
        },
        work_dir=".",
        run_from_copy=True,
        cry_copy_dir="$MY_HOME/.cryri",
    )
    assert config.environment == {
        "HF_HOME": "/mock/fake_user/.cache/huggingface",
        "WANDB_API_KEY": "8aead3118j2ej28e2jee",
        "WANDB_PROJECT": "LoRa-TinyLlama",
        "TEAM_NAME": "look/like/path",
    }
    assert config.work_dir == "/mock/fake_dir"
    assert config.cry_copy_dir == "/mock/fake_user/sub_user/.cryri"


def test_version():
    from cryri.main import app
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "0.2.0" in result.output


def test_help():
    from cryri.main import app
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "submit" in result.output
    assert "jobs" in result.output
    assert "logs" in result.output
    assert "kill" in result.output
    assert "instances" in result.output


def test_submit_missing_file():
    from cryri.main import app
    result = runner.invoke(app, ["submit", "nonexistent.yaml"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_jobs_without_client_lib():
    from cryri.main import app
    with patch("cryri.job_manager._require_client_lib") as mock_req:
        from cryri.job_manager import ClientLibMissingError
        mock_req.side_effect = ClientLibMissingError("client_lib is not installed.")
        result = runner.invoke(app, ["jobs"])
        assert result.exit_code == 1
        assert "client_lib" in result.output


def test_jobs_with_mock_data():
    from cryri.main import app
    mock_client_lib = MagicMock()

    def fake_jobs(region):
        sys.stdout.write("my-job : abc123\nanother-job : def456\n")

    mock_client_lib.jobs.side_effect = fake_jobs

    with patch("cryri.job_manager._require_client_lib", return_value=mock_client_lib):
        result = runner.invoke(app, ["jobs", "--region", "SR006"])
        assert result.exit_code == 0
        assert "my-job" in result.output
        assert "abc123" in result.output
