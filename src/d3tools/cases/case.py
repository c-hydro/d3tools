"""Case objects generated from option dictionaries.

This module defines the lightweight :class:`Case` value object used by
:class:`d3tools.cases.case_manager.CaseManager` and helper functions that expand
mapping-valued options into every possible combination.
"""

from .utils import permutate_options


class Case():
    """Represent one concrete option set together with its selected tags.

    Each case keeps a copy of the effective option mapping and the subset of tag
    values that were used to generate that branch of the case tree.

    Attributes:
        options: The fully resolved option dictionary for the case.
        alltags: The full tag dictionary for the original case family.
        tags: The tag/value pairs that correspond to the case's expanded
            dimensions.
        name: A comma-separated rendering of the selected tag assignments.
    """

    def __init__(self, options: dict, tags: dict):
        """Create a case from an option mapping and its generated tags.

        Args:
            options: The fully resolved option set for this case.
            tags: Tag/value pairs that identify how this case was selected from
                the expanded option dimensions.
        """
        self.options = options.copy()
        self.alltags = tags.copy()
        self.tags = {}

        for key in self.options:
            if key in tags:
                self.tags[key] = tags[key]

        self.name = ','.join(f'{k}={v}' for k, v in self.tags.items())

    def __str__(self):
        """Return a compact string representation of the case tags."""
        return self.name

    def __repr__(self):
        """Return a debugging representation that includes the case name."""
        return '<Case: ' + self.name + '>'

    def __add__(self, other):
        """Combine this case with another case into a new case.

        Args:
            other: A second case whose option mapping and tag dictionary should be
                merged in.

        Returns:
            Case: A new case containing the union of both mappings. Later values
            override earlier ones when the same key is present in both cases.
        """
        new_options = {**self.options, **other.options}
        new_tags = {**self.alltags, **other.alltags}
        return Case(new_options, new_tags)

    def __eq__(self, other):
        """Return whether another object describes the same option set.

        Args:
            other: Object to compare with this case.

        Returns:
            bool: True when both cases have matching option dictionaries.
        """
        return self.options == other.options


def get_cases(options):
    """Expand dictionary-valued options into a list of :class:`Case` objects.

    Args:
        options: Mapping of option names to either fixed values or dictionary
            mappings that should be expanded into dimension-specific cases.

    Returns:
        list[Case]: One case per combination of the dictionary-valued options,
        preserving the original tag metadata for each generated case.
    """
    all_options, all_tags = permutate_options(options)

    cases = []
    for opt, tags in zip(all_options, all_tags):
        cases.append(Case(opt, tags))

    return (cases)