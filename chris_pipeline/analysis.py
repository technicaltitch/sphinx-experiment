import os

import numpy as np
from luigi import ExternalTask, Task, configuration, LocalTarget

config = configuration.get_config()

xyz_q_list = {}
xyz_num_to_word = {}


class AllSectionsExternal(ExternalTask):

    def output(self):
        """
        This is a *docstring* for the output method. It includes ``code`` inline, and as a block:

        .. code-block:: python
           :linenos:
           :emphasize-lines: 2,3
           :caption: this.py
           :name: this-py

            def output(self):
                l = [1, 2, 3, ]
                i = l + [4, 5, ]
                # This should be Python highlighted and lines 2 and 3 emphasised - these don't seem to work. Hey ho.
                return [LocalTarget(os.path.join(config.get("paths", "quarterly_data_path"), "Section_6__Household.csv"))]

        It includes a doctest section. These can be automatically tested on each commit to test the
        documentation is up to date.

        >>> 1 + 1
        2

        Some tables:

        +------------------------+------------+----------+----------+
        | Header row, column 1   | Header 2   | Header 3 | Header 4 |
        | (header rows optional) |            |          |          |
        +========================+============+==========+==========+
        | body row 1, column 1   | column 2   | column 3 | column 4 |
        +------------------------+------------+----------+----------+
        | body row 2             | ...        | ...      |          |
        +------------------------+------------+----------+----------+

        Simpler:

        =====  =====  =======
        A      B      A and B
        =====  =====  =======
        False  False  False
        True   False  False
        False  True   False
        True   True   True
        =====  =====  =======

        A `link <https://www.example.com/>`_

        A `footnote too`_.

        .. _footnote too: https://example.com/

        Also, a footnoted academic reference [ref01]_

        Some text replacement |to_replace|

        .. |to_replace| replace:: replacement *replaced*

        An internal reference to our in-page :class:`ChrisAggWeighted` will convert into a hyperlink.

        An external reference to Luigi :class:`Task` will **not** convert into a hyperlink.
        You can use ``:class:``, ``:mod:`` and ``:func:`` similarly.

        .. warning::

            It also includes a warning.

        .. versionadded:: 0.30

        And a version comment.

        An image:

        .. image:: _static/logo.*
            :alt: RM logo!

        Inline maths: :math:`a^2 + b^2 = c^2`.

        Maths sections, labelled for cross-referencing:

        .. math:: e^{i\pi} + 1 = 0
           :label: euler

        Euler's identity, equation :eq:`euler`, was elected one of the most
        beautiful mathematical formulas. Unfortunately the linked to equation
        is much *less* beautiful when a ``:label:`` is specified, so don't label
        equations until readthedocs fixes this.

        .. graphviz::

             digraph example {
                 a [label="chris_pipeline.analysis.AppendDataFrames", href="chris_pipeline.html#chris_pipeline.analysis.AppendDataFrames", target="_blank"];
                 b [label="other"];
                 a -> b;
             }

        There are more complex directives
        `documented here. <https://docutils.readthedocs.io/en/sphinx-docs/ref/rst/directives.html>`_

        For example the inheritance diagram for ChrisAggWeighted:

        .. inheritance-diagram:: chris_pipeline.analysis.ChrisAggWeighted

        For example the pipeline DAG diagram for ChrisAggWeighted:

        .. pipeline-diagram:: chris_pipeline.analysis.ChrisAggWeighted

        :return: :class:`ChrisAggWeighted`

        .. [ref01] https://www.example.com
        """
        return [LocalTarget(os.path.join(config.get("paths", "quarterly_data_path"), "Section_6__Household.csv"))]


class ReadAllSections(Task):

    def requires(self):
        return AllSectionsExternal()


class ChrisCalcTask(Task):
    """
    This is a docstring for ChrisCalcTask
    """

    def requires(self):
        return ReadAllSections()

    def output(self):
        return object()

    def run(self):
        """
        Calculate the things. Return dataframe with data inside. docstring for a method
        """
        data_dict = self.input().get()['clean_data']
        df = data_dict['xyz'].copy()

        self.output().put(df)

    def func_xyz(self, temp_df, xyz_q_list):
        """
        We could use standard RST docstrings:

            :param temp_df: The survey response DataFrame
            :type temp_df: DataFrame
            :param xyz_q_list: The :class:`list` of questions for xyz
            :type xyz_q_list: list
            :returns: A list of processed answers
            :rtype: list

        Or the Google style - more readable and rendered with the Sphinx napoleon plugin:

        Args:
            temp_df (DataFrame): The survey response DataFrame
            xyz_q_list (list): The :class:`list` of questions for xyz

        Returns:
            list: A :class:`list` of processed answers

        Or the Numpy style - more readable when we've lots of parameters:

        Parameters
        ----------
        temp_df : DataFrame
            The survey response ::class::`DataFrame`
        xyz_q_list : list
            The :class:`list` of questions for xyz

        Returns
        -------
        list
            The :class:`list` of processed answers

        Possible section headers:

            * ``Args`` (alias of Parameters)
            * ``Arguments`` (alias of Parameters)
            * ``Attributes``
            * ``Example``
            * ``Examples``
            * ``Keyword Args`` (alias of Keyword Arguments)
            * ``Keyword Arguments``
            * ``Methods``
            * ``Note``
            * ``Notes``
            * ``Other Parameters``
            * ``Parameters``
            * ``Return`` (alias of Returns)
            * ``Returns``
            * ``Raises``
            * ``References``
            * ``See Also``
            * ``Todo``
            * ``Warning``
            * ``Warnings`` (alias of Warning)
            * ``Warns``
            * ``Yield`` (alias of Yields)
            * ``Yields``
        """
        for q in xyz_q_list:
            temp_df = temp_df.replace({q: {2: 1}})
            temp_df = temp_df.replace({q: {3: 2}})
        xyz_num_list = temp_df[xyz_q_list].sum(axis=1)

        return xyz_num_list


class ChrisAggWeighted(Task):
    """
    Docstring for ChrisAggWeighted
    """

    def requires(self):
        return ChrisCalcTask()

    def output(self):
        return None

    def run(self):
        """
        Aggregate data and apply population weightings if necessary.
        """
        self.output().put(self.input.get())


class ChrisAggUnweighted(Task):
    """
    Aggregate county level data that doesn't need to be weighted.
    """

    def requires(self):
        return ChrisCalcTask()

    def output(self):
        return LocalTarget(name='aggregated_unweighted_data', timeout=10)

    def run(self):
        """
        Aggregate data and apply population weightings if necessary.
        """
        df = self.input().get()
        self.output().put(df)


class AppendDataFrames(Task):
    """
    Some docstring for appending dataframes
    """

    def requires(self):
        return ChrisAggUnweighted(), ChrisAggWeighted()

    def output(self):
        return LocalTarget(name='appended_data', timeout=10)

    def run(self):
        df_lev1 = self.input()[0].get()
        df_lev0 = self.input()[1].get()

        df_lev1 = df_lev1.reset_index()
        df_lev0 = df_lev0.reset_index()

        df_lev0['report_region_lev1'] = df_lev0['report_region_lev0']
        df = df_lev1.append(df_lev0)

        self.output().put(df)


class CalculateConfidenceIntervals(Task):
    """
    Calculate confidence intervals.
    """

    def requires(self):
        return AppendDataFrames()

    def output(self):
        return LocalTarget(name='data_confidence_intervals', timeout=10)

    def run(self):

        df = self.input().get()
        df['CI'] = list(map(
            lambda x, y: self.calculate_CI(x, y), df['Percent'], df['Total'])
        )

        self.output().put(df)

    def calculate_CI(self, val, ss, percentage=True, conf=1.96):
        """
        Calculate the confidence interval
        val = calculated percentage if percentage=True, standard deviation if percentage = False (continuous variables)
        ss = sample size
        conf = z-score for desired confidence interval, set to 95% by default
        precentage = True if the value is a percentage, False if value is a real number or integer (i.e. mean)
        """

        if percentage is True:
            try:
                ci = (np.sqrt((val * (1 - val)) / ss)) * conf
            except ZeroDivisionError:
                ci = np.nan

        elif percentage is False:
            try:
                ci = (val / np.sqrt(ss)) * conf
            except ZeroDivisionError:
                ci = np.nan

        return ci
