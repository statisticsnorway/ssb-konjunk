"""A collection of functions to make xml files handling easier both in dapla and prodsone."""

import xml.etree.ElementTree as ET


def read_xml(xml_file: str) -> ET.Element:
    """Funtion to get xml root from disk.

    Args:
        xml_file: Strin value for xml filepath.

    Returns:
        ET.Element: Root of xml file.
    """
    with open(xml_file) as file:
        single_xml = file.read()
        file.close()

    return ET.fromstring(single_xml)


def return_txt_xml(root: ET.Element, child: str) -> str | None:
    """Function to return text value from child element in xml file.

    Args:
        root: Root with all data stored in a branch like structure.
        child: String value to find child element which contains a value.

    Returns:
        str: Returns string value from child element.
    """
    string = None
    for element in root.iter(child):
        string = element.text
    return string


def dump_element(element: ET.Element, indent: int = 0) -> None:
    """Function to print xml in pretty format.

    Args:
        element: ET.Element you want to print.
        indent: Level of ident you want.
    """
    print("  " * indent + f"Tag: {element.tag}")
    if element.text and element.text.strip():
        print("  " * (indent + 1) + f"Text: {element.text.strip()}")
    for attribute, value in element.attrib.items():
        print("  " * (indent + 1) + f"Attribute: {attribute}={value}")
    for child in element:
        dump_element(child, indent + 1)
