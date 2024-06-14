"""A collection of functions to make xml files handling easier both in dapla and prodsone.
"""

import pandas as pd
import xml.etree.ElementTree as ET


def read_xml(xml_file: str, fs=None) -> ET.Element:
    """Funtion to get xml root from disk.
    
    Args:
        xml_file: Strin value for xml filepath.
        fs: filesystem
    
    Returns:
        ET.Element: Root of xml file."""
    
    if fs:
        with fs.open(xml_file, mode="r") as file:
            single_xml = file.read()
            file.close()
    else:
        with open(xml_file, mode="r") as file:
            single_xml = file.read()
            file.close()
            
    return ET.fromstring(single_xml)


def return_txt_xml(root: ET.Element, child: str) -> str|None:
    """
    Function to return text value from child element in xml file.

    Args:
        root: Root with all data stored in a branch like structure.
        child: String value to find child element which contains a value.

    Returns:
        str: Returns string value from child element.
    """
    for element in root.iter(child):
        return element.text




