import pytest
from src.sqlplate import SQLPlate


def test_sqlplate_raise(template_path):
    with pytest.raises(FileNotFoundError):
        SQLPlate.format(name='not-exists', path=template_path / 'not-exists')

    with pytest.raises(FileNotFoundError):
        SQLPlate.format(name='not-exists', path=template_path)


def test_sqlplate_formats(template_path):
    formats: list[str] = SQLPlate.formats(path=template_path)
    assert isinstance(formats, list)
    assert len(formats) > 0
    assert 'utils' not in formats
