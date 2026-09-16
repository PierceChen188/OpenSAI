"""
CLI parsing tests (APP-1.10).

Covers: APP-1.8 subcommand registration, --help/--version, argument parsing.
"""

import sys
import io
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def test_cli_imports():
    from opensai_client.cli import main, _build_parser, __version__
    assert __version__ == '0.1.0'
    assert callable(main)
    assert callable(_build_parser)


def test_cli_parser_builds():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    assert parser is not None


def test_cli_help():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        parser.parse_args(['--help'])
    except SystemExit:
        pass
    output = sys.stdout.getvalue()
    sys.stdout = old_stdout
    assert 'opensai' in output.lower() or 'usage' in output.lower()


def test_cli_version():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    try:
        parser.parse_args(['--version'])
    except SystemExit:
        pass


def test_cli_algorithms_list():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['algorithms', 'list'])
    assert args.command == 'algorithms'
    assert args.algo_action == 'list'


def test_cli_devices_list():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['devices', 'list'])
    assert args.command == 'devices'


def test_cli_cluster_status():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['cluster', 'status'])
    assert args.command == 'cluster'


def test_cli_dag_validate_file():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['dag', 'validate', '--file', 'test.json'])
    assert args.dag_action == 'validate'
    assert args.file == 'test.json'


def test_cli_dag_submit_file():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['dag', 'submit', '--file', 'dag.json'])
    assert args.dag_action == 'submit'
    assert args.file == 'dag.json'


def test_cli_result_get():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['result', 'get', 'job_abc123'])
    assert args.job_id == 'job_abc123'


def test_cli_result_wait():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['result', 'wait', 'job_xyz', '--timeout', '120'])
    assert args.job_id == 'job_xyz'
    assert args.timeout == 120


def test_cli_task_submit():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args([
        'task', 'submit', 'Algorithm_Pi_Calculation',
        '--params', '{"count_num": 100}',
        '--timeout', '30',
    ])
    assert args.algorithm == 'Algorithm_Pi_Calculation'
    assert args.params == '{"count_num": 100}'
    assert args.timeout == 30


def test_cli_default_host_port():
    from opensai_client.cli import _build_parser
    parser = _build_parser()
    args = parser.parse_args(['algorithms', 'list'])
    assert args.host == '127.0.0.1'
    assert args.port == 8888
