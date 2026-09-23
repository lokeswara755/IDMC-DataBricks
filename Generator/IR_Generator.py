from typing import Any, Dict, List, Optional


class IRGenerator:
    """
    Converts parsed IDMC metadata + graph information
    into a canonical Intermediate Representation (IR).

    The IR is consumed by:
        - IRValidator
        - PySparkGenerator
        - Migration pipeline
    """

    def generate(
        self,
        repository: Dict[str, Any],
        folder: Dict[str, Any],
        mapping: Dict[str, Any],
        graph: Dict[str, Any]
    ) -> Dict[str, Any]:

        sources = self._build_sources(
            repository,
            folder,
            mapping
        )

        target = self._build_target(
            repository,
            folder,
            mapping
        )

        # The compact target representation may not carry physical
        # target columns. The graph, however, contains the actual
        # target instance and incoming target ports. Preserve those
        # ports in the canonical IR so the validator and generator
        # can validate/map the target correctly.
        target = self._enrich_target_ports(
            target,
            graph
        )

        transformations = self._build_transformations(
            mapping,
            graph
        )

        links = self._build_links(
            mapping,
            graph
        )

        source_instances = self._build_source_instances(
            mapping,
            repository,
            folder
        )

        return {
            "name": mapping.get("name"),
            "description": mapping.get("description"),

            # Physical source definitions
            "sources": sources,

            # IMPORTANT:
            # Preserve IDMC source-instance information.
            #
            # Example:
            # SRC_CHANNEL -> CHANNEL_MASTER
            # SRC_CUSTOMER -> CUSTOMER
            # SRC_ORDERS -> ORDERS
            #
            # PySparkGenerator uses this information to resolve
            # SOURCE_QUALIFIER -> physical source table.
            "source_instances": source_instances,

            # Transformations
            "transformations": transformations,

            # Target
            "target": target,

            # Mapping links
            "links": links,

            # Graph execution order
            "execution_order": graph.get(
                "execution_order",
                []
            ),

            # Session information
            "session": self._build_session(
                folder.get("sessions", []),
                mapping.get("name")
            )
        }

    # ------------------------------------------------------------------
    # SOURCES
    # ------------------------------------------------------------------

    def _build_sources(
        self,
        repository: Dict[str, Any],
        folder: Dict[str, Any],
        mapping: Dict[str, Any]
    ) -> List[Dict[str, Any]]:

        sources: List[Dict[str, Any]] = []
        all_sources: List[Any] = []

        # Collect physical source definitions from every parser level.
        for container in (repository, folder, mapping):
            if not isinstance(container, dict):
                continue

            for key in (
                "sources",
                "source_definitions",
                "source_tables"
            ):
                value = container.get(key, [])
                if isinstance(value, list):
                    all_sources.extend(value)

        source_lookup: Dict[str, Dict[str, Any]] = {}

        for source in all_sources:
            if not isinstance(source, dict):
                continue

            source_name = (
                source.get("name")
                or source.get("source_name")
                or source.get("definition")
                or source.get("table")
            )

            if source_name:
                source_lookup[
                    str(source_name).strip().lower()
                ] = source

        # The mapping contains the important SRC_* -> physical source
        # relationship. Always use it to build the canonical source list.
        source_instances = self._build_source_instances(
            mapping,
            repository,
            folder
        )

        for instance in source_instances:
            instance_name = instance.get("name")
            source_name = instance.get("source") or instance_name

            if not source_name:
                continue

            source_definition = source_lookup.get(
                str(source_name).strip().lower()
            )

            if source_definition:
                source_ir = dict(source_definition)
            else:
                source_ir = {"name": source_name}

            source_ir["name"] = source_name
            source_ir["instance"] = instance_name
            sources.append(source_ir)

        # Fallback to mapping.sources if no source instances were found.
        if not sources:
            mapping_sources = mapping.get("sources", [])

            if isinstance(mapping_sources, list):
                for item in mapping_sources:
                    if isinstance(item, str):
                        source_name = item
                        instance_name = item
                    elif isinstance(item, dict):
                        source_name = (
                            item.get("source")
                            or item.get("source_name")
                            or item.get("name")
                            or item.get("definition")
                        )
                        instance_name = (
                            item.get("instance")
                            or item.get("source_instance")
                            or item.get("name")
                            or source_name
                        )
                    else:
                        continue

                    if not source_name:
                        continue

                    source_definition = source_lookup.get(
                        str(source_name).strip().lower()
                    )

                    source_ir = (
                        dict(source_definition)
                        if source_definition
                        else {"name": source_name}
                    )

                    source_ir["name"] = source_name
                    source_ir["instance"] = instance_name
                    sources.append(source_ir)

        # Final fallback to physical source definitions.
        if not sources:
            for source in all_sources:
                if not isinstance(source, dict):
                    continue

                source_name = (
                    source.get("name")
                    or source.get("source_name")
                    or source.get("definition")
                    or source.get("table")
                )

                if source_name:
                    source_ir = dict(source)
                    source_ir["name"] = source_name
                    source_ir.setdefault("instance", source_name)
                    sources.append(source_ir)

        # Deduplicate.
        result = []
        seen = set()

        for source in sources:
            name = source.get("name")
            if not name:
                continue

            key = str(name).strip().lower()

            if key not in seen:
                seen.add(key)
                result.append(source)

        return result

    # ------------------------------------------------------------------
    # SOURCE INSTANCES
    # ------------------------------------------------------------------

    def _build_source_instances(
        self,
        mapping: Dict[str, Any],
        repository: Dict[str, Any],
        folder: Dict[str, Any]
    ) -> List[Dict[str, Any]]:

        """
        Normalize source-instance relationships.

        Expected:
            SRC_CUSTOMER -> CUSTOMER
            SRC_ORDERS   -> ORDERS
            SRC_COUNTRY  -> COUNTRY_MASTER
            SRC_SEGMENT  -> SEGMENT_MASTER
            SRC_CHANNEL  -> CHANNEL_MASTER
        """

        raw_items = []

        for container in (mapping, folder, repository):
            if not isinstance(container, dict):
                continue

            for key in (
                "source_instances",
                "sourceInstances",
                "source_instances_list"
            ):
                value = container.get(key)
                if isinstance(value, list):
                    raw_items.extend(value)

        result = []
        seen = set()

        for item in raw_items:

            if isinstance(item, str):
                instance_name = item
                source_name = item

            elif isinstance(item, dict):
                instance_name = (
                    item.get("name")
                    or item.get("instance")
                    or item.get("source_instance")
                    or item.get("sourceInstance")
                    or item.get("instance_name")
                )

                source_name = (
                    item.get("source")
                    or item.get("source_name")
                    or item.get("sourceName")
                    or item.get("definition")
                    or item.get("source_definition")
                    or item.get("table")
                    or item.get("object")
                )

                # Support nested source dictionaries.
                if isinstance(source_name, dict):
                    source_name = (
                        source_name.get("name")
                        or source_name.get("source_name")
                        or source_name.get("table")
                    )

            else:
                continue

            if not instance_name:
                continue

            if not source_name:
                source_name = instance_name

            key = (
                str(instance_name).strip().lower(),
                str(source_name).strip().lower()
            )

            if key in seen:
                continue

            seen.add(key)

            result.append(
                {
                    "name": instance_name,
                    "source": source_name
                }
            )

        # Some parsers store source-instance dictionaries in mapping.sources.
        if not result:
            mapping_sources = mapping.get("sources", [])

            if isinstance(mapping_sources, list):
                for item in mapping_sources:

                    if not isinstance(item, dict):
                        continue

                    instance_name = (
                        item.get("instance")
                        or item.get("source_instance")
                        or item.get("name")
                    )

                    source_name = (
                        item.get("source")
                        or item.get("source_name")
                        or item.get("definition")
                    )

                    if not instance_name:
                        continue

                    if not source_name:
                        source_name = instance_name

                    result.append(
                        {
                            "name": instance_name,
                            "source": source_name
                        }
                    )

        return result

    # ------------------------------------------------------------------
    # TARGET
    # ------------------------------------------------------------------

    def _get_graph_links(self, graph: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Return graph connections using either the canonical ``links``
        key or the GraphBuilder ``edges`` key.

        GraphBuilder currently exposes connections as ``edges`` while
        some callers may provide ``links``. The IR generator must accept
        both shapes so port discovery and link generation remain generic.
        """
        if not isinstance(graph, dict):
            return []

        graph_links = graph.get("links")
        if isinstance(graph_links, list):
            return [
                dict(link)
                for link in graph_links
                if isinstance(link, dict)
            ]

        graph_edges = graph.get("edges")
        if isinstance(graph_edges, list):
            normalized = []

            for edge in graph_edges:
                if not isinstance(edge, dict):
                    continue

                normalized.append({
                    "from": edge.get("from"),
                    "from_port": edge.get("from_port"),
                    "to": edge.get("to"),
                    "to_port": edge.get("to_port"),
                    "attributes": edge.get("attributes", {})
                })

            return normalized

        return []

    def _enrich_target_ports(
        self,
        target: Optional[Dict[str, Any]],
        graph: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Ensure the target instance contains ports referenced by graph
        links. Some parser/IR shapes keep target columns separately,
        while the compact mapping target may only contain name/target.
        """

        if not isinstance(target, dict):
            return target

        existing_ports = target.get("ports", [])
        if not isinstance(existing_ports, list):
            existing_ports = []

        normalized_ports = []
        existing_names = set()

        for port in existing_ports:
            if not isinstance(port, dict):
                continue

            name = port.get("name")
            if not name:
                continue

            normalized_ports.append(dict(port))
            existing_names.add(str(name).strip().lower())

        target_instance = (
            target.get("instance_name")
            or target.get("name")
        )

        graph_links = self._get_graph_links(graph)

        for link in graph_links:
            if not isinstance(link, dict):
                continue

            to_node = (
                link.get("to_node")
                or link.get("to")
            )

            to_port = link.get("to_port")

            if (
                target_instance
                and to_node == target_instance
                and to_port
            ):
                key = str(to_port).strip().lower()

                if key not in existing_names:
                    normalized_ports.append({
                        "name": to_port,
                        "direction": "INPUT"
                    })
                    existing_names.add(key)

        result = dict(target)
        result["ports"] = normalized_ports
        return result

    def _build_target(
        self,
        repository: Dict[str, Any],
        folder: Dict[str, Any],
        mapping: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:

        """
        Normalize the target from whichever parser level contains it.
        """

        def normalize(item):
            if isinstance(item, str):
                return {"name": item}

            if not isinstance(item, dict):
                return None

            # Direct target.
            name = (
                item.get("name")
                or item.get("target_name")
                or item.get("targetName")
                or item.get("table")
            )

            # Target instance may contain the actual target definition.
            nested = item.get("target")

            if isinstance(nested, dict):
                nested_name = (
                    nested.get("name")
                    or nested.get("target_name")
                    or nested.get("table")
                )

                if nested_name:
                    result = dict(nested)
                    result["name"] = nested_name
                    result.setdefault(
                        "instance_name",
                        name
                    )
                    return result

            if isinstance(nested, str) and not name:
                name = nested

            if name:
                result = dict(item)
                result["name"] = name
                return result

            return None

        # Prefer mapping-level target information.
        for key in (
            "target",
            "targets",
            "target_instances",
            "targetInstances"
        ):
            value = mapping.get(key)

            if isinstance(value, list):
                for item in value:
                    result = normalize(item)
                    if result:
                        return result

            elif value is not None:
                result = normalize(value)
                if result:
                    return result

        # Then folder/repository target definitions.
        for container in (folder, repository):
            if not isinstance(container, dict):
                continue

            for key in (
                "targets",
                "target_definitions",
                "target_instances",
                "targetInstances"
            ):
                value = container.get(key)

                if isinstance(value, list):
                    for item in value:
                        result = normalize(item)
                        if result:
                            return result

        return None

    # ------------------------------------------------------------------
    # TRANSFORMATIONS
    # ------------------------------------------------------------------

    def _build_transformations(
        self,
        mapping: Dict[str, Any],
        graph: Dict[str, Any]
    ) -> List[Dict[str, Any]]:

        transformations = mapping.get(
            "transformations",
            []
        )

        if not isinstance(
            transformations,
            list
        ):
            return []

        result = []

        for transformation in transformations:

            if not isinstance(
                transformation,
                dict
            ):
                continue

            transformation_ir = dict(
                transformation
            )

            name = transformation.get(
                "name"
            )

            transformation_type = (
                transformation.get(
                    "type"
                )
                or transformation.get(
                    "transformation_type"
                )
                or transformation.get(
                    "object_type"
                )
            )

            transformation_ir[
                "name"
            ] = name

            transformation_ir[
                "type"
            ] = transformation_type

            transformation_ir[
                "ports"
            ] = self._normalize_ports(
                transformation,
                graph
            )

            transformation_ir[
                "config"
            ] = self._build_config(
                transformation
            )

            result.append(
                transformation_ir
            )

        return result

    # ------------------------------------------------------------------
    # PORT NORMALIZATION
    # ------------------------------------------------------------------

    def _normalize_ports(
        self,
        transformation: Dict[str, Any],
        graph: Dict[str, Any]
    ) -> List[Dict[str, Any]]:

        transformation_name = transformation.get(
            "name"
        )

        original_ports = transformation.get(
            "ports",
            []
        )

        if not isinstance(
            original_ports,
            list
        ):
            original_ports = []

        normalized = []

        existing_names = set()

        # --------------------------------------------------------------
        # Step 1:
        # Normalize existing ports.
        # --------------------------------------------------------------

        for port in original_ports:

            if not isinstance(
                port,
                dict
            ):
                continue

            port_ir = dict(
                port
            )

            port_name = port_ir.get(
                "name"
            )

            if not port_name:
                continue

            port_ir["name"] = port_name

            direction = port_ir.get(
                "direction"
            )

            if not direction:
                direction = self._infer_port_direction(
                    transformation_name,
                    port_name,
                    graph
                )

            port_ir[
                "direction"
            ] = direction

            normalized.append(
                port_ir
            )

            existing_names.add(
                str(port_name).strip().lower()
            )

        # --------------------------------------------------------------
        # Step 2:
        # Scan graph links and add missing ports.
        # --------------------------------------------------------------

        graph_links = self._get_graph_links(graph)

        for link in graph_links:

            if not isinstance(
                link,
                dict
            ):
                continue

            from_node = (
                link.get("from_node")
                or link.get("from")
            )

            to_node = (
                link.get("to_node")
                or link.get("to")
            )

            from_port = link.get(
                "from_port"
            )

            to_port = link.get(
                "to_port"
            )

            # ----------------------------------------------------------
            # Incoming port
            # ----------------------------------------------------------

            if (
                to_node == transformation_name
                and to_port
            ):

                key = str(
                    to_port
                ).strip().lower()

                if key not in existing_names:

                    normalized.append(
                        {
                            "name": to_port,
                            "direction": "INPUT"
                        }
                    )

                    existing_names.add(
                        key
                    )

            # ----------------------------------------------------------
            # Outgoing port
            # ----------------------------------------------------------

            if (
                from_node == transformation_name
                and from_port
            ):

                key = str(
                    from_port
                ).strip().lower()

                if key not in existing_names:

                    normalized.append(
                        {
                            "name": from_port,
                            "direction": "OUTPUT"
                        }
                    )

                    existing_names.add(
                        key
                    )

        # --------------------------------------------------------------
        # Step 3:
        # Update directions based on graph usage.
        # --------------------------------------------------------------

        for port in normalized:

            port_name = port.get(
                "name"
            )

            if not port_name:
                continue

            graph_direction = (
                self._direction_from_usage(
                    transformation_name,
                    port_name,
                    graph
                )
            )

            if graph_direction:
                port[
                    "direction"
                ] = graph_direction

        return normalized

    # ------------------------------------------------------------------
    # PORT DIRECTION
    # ------------------------------------------------------------------

    def _infer_port_direction(
        self,
        transformation_name: Optional[str],
        port_name: Optional[str],
        graph: Dict[str, Any]
    ) -> str:

        direction = self._direction_from_usage(
            transformation_name,
            port_name,
            graph
        )

        if direction:
            return direction

        return "OUTPUT"

    def _direction_from_usage(
        self,
        transformation_name: Optional[str],
        port_name: Optional[str],
        graph: Dict[str, Any]
    ) -> Optional[str]:

        if not transformation_name:
            return None

        if not port_name:
            return None

        graph_links = self._get_graph_links(graph)

        if not graph_links:
            return None

        is_input = False
        is_output = False

        for link in graph_links:

            if not isinstance(
                link,
                dict
            ):
                continue

            to_node = (
                link.get("to_node")
                or link.get("to")
            )

            from_node = (
                link.get("from_node")
                or link.get("from")
            )

            if (
                to_node
                == transformation_name
                and link.get("to_port")
                == port_name
            ):
                is_input = True

            if (
                from_node
                == transformation_name
                and link.get("from_port")
                == port_name
            ):
                is_output = True

        if is_input and is_output:
            return "INPUT_OUTPUT"

        if is_input:
            return "INPUT"

        if is_output:
            return "OUTPUT"

        return None

    # ------------------------------------------------------------------
    # CONFIGURATION
    # ------------------------------------------------------------------

    def _build_config(
        self,
        transformation: Dict[str, Any]
    ) -> Dict[str, Any]:

        transformation_type = str(
            transformation.get(
                "type",
                ""
            )
        ).upper()

        config = {}

        # --------------------------------------------------------------
        # Existing parser config
        # --------------------------------------------------------------

        existing_config = transformation.get(
            "config"
        )

        if isinstance(
            existing_config,
            dict
        ):
            config.update(
                existing_config
            )

        # --------------------------------------------------------------
        # FILTER
        # --------------------------------------------------------------

        if transformation_type == "FILTER":

            condition = (
                transformation.get(
                    "condition"
                )
                or transformation.get(
                    "filter_condition"
                )
                or config.get(
                    "condition"
                )
            )

            if condition:
                config[
                    "condition"
                ] = condition

        # --------------------------------------------------------------
        # LOOKUP
        # --------------------------------------------------------------

        elif transformation_type == "LOOKUP":

            lookup_condition = (
                transformation.get(
                    "condition"
                )
                or transformation.get(
                    "lookup_condition"
                )
                or config.get(
                    "lookup_condition"
                )
            )

            lookup_source = (
                transformation.get(
                    "lookup_source"
                )
                or config.get(
                    "lookup_source"
                )
            )

            if lookup_condition:
                config[
                    "lookup_condition"
                ] = lookup_condition

            if lookup_source:
                config[
                    "lookup_source"
                ] = lookup_source

            if not lookup_source and lookup_condition:
                derived_source = (
                    self._derive_lookup_source(
                        lookup_condition
                    )
                )

                if derived_source:
                    config[
                        "lookup_source"
                    ] = derived_source

        # --------------------------------------------------------------
        # JOINER
        # --------------------------------------------------------------

        elif transformation_type == "JOINER":

            join_condition = (
                transformation.get(
                    "condition"
                )
                or transformation.get(
                    "join_condition"
                )
                or config.get(
                    "join_condition"
                )
            )

            join_type = (
                transformation.get(
                    "join_type"
                )
                or config.get(
                    "join_type"
                )
            )

            if join_condition:
                config[
                    "join_condition"
                ] = join_condition

            if join_type:
                config[
                    "join_type"
                ] = join_type

        # --------------------------------------------------------------
        # AGGREGATOR
        # --------------------------------------------------------------

        elif transformation_type == "AGGREGATOR":

            group_by = (
                transformation.get(
                    "group_by"
                )
                or config.get(
                    "group_by"
                )
            )

            if group_by:
                config[
                    "group_by"
                ] = group_by

        # --------------------------------------------------------------
        # SOURCE QUALIFIER
        # --------------------------------------------------------------

        elif transformation_type == "SOURCE_QUALIFIER":

            input_source = (
                transformation.get(
                    "input_source"
                )
                or transformation.get(
                    "source"
                )
                or config.get(
                    "input_source"
                )
                or config.get(
                    "source"
                )
            )

            if input_source:
                config[
                    "input_source"
                ] = input_source

        return config

    # ------------------------------------------------------------------
    # LOOKUP SOURCE DERIVATION
    # ------------------------------------------------------------------

    def _derive_lookup_source(
        self,
        condition: str
    ) -> Optional[str]:

        if not condition:
            return None

        text = str(
            condition
        ).strip()

        if "=" not in text:
            return None

        left, right = text.split(
            "=",
            1
        )

        left = left.strip()
        right = right.strip()

        if "." in left:
            return left.split(
                ".",
                1
            )[0].strip()

        if "." in right:
            return right.split(
                ".",
                1
            )[0].strip()

        return None

    # ------------------------------------------------------------------
    # LINKS
    # ------------------------------------------------------------------

    def _build_links(
        self,
        mapping: Dict[str, Any],
        graph: Dict[str, Any]
    ) -> List[Dict[str, Any]]:

        graph_links = self._get_graph_links(graph)

        if graph_links:
            return graph_links

        mapping_links = mapping.get(
            "links",
            []
        )

        if not isinstance(
            mapping_links,
            list
        ):
            return []

        result = []

        for link in mapping_links:

            if isinstance(
                link,
                dict
            ):
                result.append(
                    dict(link)
                )

        return result

    # ------------------------------------------------------------------
    # SESSION
    # ------------------------------------------------------------------

    def _build_session(
        self,
        sessions: Any,
        mapping_name: Optional[str]
    ) -> Dict[str, Any]:

        if not isinstance(
            sessions,
            list
        ):
            return {}

        # First try exact mapping/session relationship.
        for session in sessions:

            if not isinstance(
                session,
                dict
            ):
                continue

            session_mapping = (
                session.get(
                    "mapping"
                )
                or session.get(
                    "mapping_name"
                )
            )

            if (
                mapping_name
                and session_mapping
                and session_mapping == mapping_name
            ):
                return dict(
                    session
                )

        # Otherwise return the first session.
        if sessions:

            first_session = sessions[0]

            if isinstance(
                first_session,
                dict
            ):
                return dict(
                    first_session
                )

        return {}