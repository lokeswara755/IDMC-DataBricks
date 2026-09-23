import xml.etree.ElementTree as ET


class XMLReader:

    def __init__(self, xml_path):
        self.xml_path = xml_path

    def read(self):

        tree = ET.parse(self.xml_path)

        root = tree.getroot()

        return root

