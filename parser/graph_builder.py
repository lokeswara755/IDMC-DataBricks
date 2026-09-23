class GraphBuilder:

    def build(self, mapping):

        nodes = []
        edges = []

        # =========================================================
        # TRANSFORMATION NODES
        # =========================================================

        for transformation in mapping.get(
            "transformations",
            []
        ):

            transformation_name = transformation.get(
                "name"
            )

            if not transformation_name:
                continue

            if transformation_name not in nodes:

                nodes.append(
                    transformation_name
                )

        # =========================================================
        # TARGET NODES
        # =========================================================

        for target_instance in mapping.get(
            "target_instances",
            []
        ):

            target_name = target_instance.get(
                "name"
            )

            if not target_name:
                continue

            if target_name not in nodes:

                nodes.append(
                    target_name
                )

        # =========================================================
        # BUILD EDGES FROM XML LINKS
        # =========================================================

        for link in mapping.get(
            "links",
            []
        ):

            from_value = link.get("from")
            to_value = link.get("to")

            if not from_value or not to_value:
                continue

            from_parts = from_value.split(
                ".",
                1
            )

            to_parts = to_value.split(
                ".",
                1
            )

            if len(from_parts) != 2:

                raise ValueError(
                    f"Invalid link source format: "
                    f"{from_value}"
                )

            if len(to_parts) != 2:

                raise ValueError(
                    f"Invalid link target format: "
                    f"{to_value}"
                )

            from_node = from_parts[0]
            from_port = from_parts[1]

            to_node = to_parts[0]
            to_port = to_parts[1]

            # -----------------------------------------------------
            # Validate only execution nodes
            # -----------------------------------------------------

            if from_node not in nodes:

                raise ValueError(
                    f"Link references unknown execution node: "
                    f"{from_node}"
                )

            if to_node not in nodes:

                raise ValueError(
                    f"Link references unknown execution node: "
                    f"{to_node}"
                )

            edge = {
                "from": from_node,
                "from_port": from_port,
                "to": to_node,
                "to_port": to_port,
                "attributes": dict(
                    link.get(
                        "attributes",
                        {}
                    )
                )
            }

            edges.append(
                edge
            )

        # =========================================================
        # TOPOLOGICAL SORT
        # =========================================================

        execution_order = self._topological_sort(
            nodes,
            edges
        )

        return {
            "nodes": nodes,
            "edges": edges,
            "execution_order": execution_order
        }

    # =========================================================
    # TOPOLOGICAL SORT
    # =========================================================

    def _topological_sort(
        self,
        nodes,
        edges
    ):

        dependencies = {
            node: set()
            for node in nodes
        }

        for edge in edges:

            source = edge["from"]
            target = edge["to"]

            if (
                source in dependencies
                and target in dependencies
            ):

                dependencies[target].add(
                    source
                )

        order = []

        remaining = {
            node: set(dependencies[node])
            for node in nodes
        }

        while remaining:

            ready = [
                node
                for node, deps in remaining.items()
                if not deps
            ]

            if not ready:

                raise ValueError(
                    "Cycle detected in mapping graph"
                )

            ready.sort()

            for node in ready:

                order.append(
                    node
                )

                del remaining[node]

            for deps in remaining.values():

                deps.difference_update(
                    ready
                )

        return order