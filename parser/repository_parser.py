from parser.connector_parser import ConnectorParser
from parser.source_parser import SourceParser
from parser.target_parser import TargetParser
from parser.mapping_parser import MappingParser
from parser.session_parser import SessionParser


class RepositoryParser:

    def __init__(self):

        self.connector_parser = ConnectorParser()
        self.source_parser = SourceParser()
        self.target_parser = TargetParser()
        self.mapping_parser = MappingParser()
        self.session_parser = SessionParser()

    def parse(self, root):

        repository = root.find("Repository")

        if repository is None:
            raise ValueError(
                "Repository element not found"
            )

        repository_data = {
            "name": repository.get("name"),
            "attributes": dict(repository.attrib),
            "folders": []
        }

        for folder in repository.findall("Folder"):

            folder_data = self._parse_folder(folder)

            repository_data["folders"].append(
                folder_data
            )

        return repository_data

    # =========================================================
    # FOLDER
    # =========================================================

    def _parse_folder(self, folder):

        folder_data = {
            "name": folder.get("name"),
            "attributes": dict(folder.attrib),

            "connections": [],
            "sources": [],
            "targets": [],
            "mappings": [],
            "sessions": [],

            # Preserve unsupported/unhandled sections
            # instead of silently throwing them away.
            "extensions": []
        }

        # -----------------------------------------------------
        # CONNECTIONS
        # -----------------------------------------------------

        folder_data["connections"] = (
            self.connector_parser.parse(folder)
        )

        # -----------------------------------------------------
        # SOURCES
        # -----------------------------------------------------

        folder_data["sources"] = (
            self.source_parser.parse(folder)
        )

        # -----------------------------------------------------
        # TARGETS
        # -----------------------------------------------------

        folder_data["targets"] = (
            self.target_parser.parse(folder)
        )

        # -----------------------------------------------------
        # MAPPINGS
        # -----------------------------------------------------

        folder_data["mappings"] = (
            self.mapping_parser.parse(folder)
        )

        # -----------------------------------------------------
        # SESSIONS
        # -----------------------------------------------------

        folder_data["sessions"] = (
            self.session_parser.parse(folder)
        )

        # -----------------------------------------------------
        # PRESERVE UNKNOWN FOLDER SECTIONS
        # -----------------------------------------------------

        known_sections = {
            "Connections",
            "Sources",
            "Targets",
            "Mappings",
            "Sessions"
        }

        for child in folder:

            if child.tag not in known_sections:

                folder_data["extensions"].append(
                    self._element_to_dict(child)
                )

        return folder_data

    # =========================================================
    # GENERIC XML ELEMENT CONVERTER
    # =========================================================

    def _element_to_dict(self, element):

        data = {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "children": []
        }

        if element.text and element.text.strip():

            data["text"] = element.text.strip()

        for child in element:

            data["children"].append(
                self._element_to_dict(child)
            )

        return data
