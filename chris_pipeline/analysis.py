import os

import numpy as np
from luigi import ExternalTask, Task, configuration

from pipelines import ReadDataFrameTask
from pipelines.targets import ExpiringMemoryTarget, LocalTarget

config = configuration.get_config()

xyz_q_list = {}
xyz_num_to_word = {}


class AllSectionsExternal(ExternalTask):

    def output(self):
        return [LocalTarget(os.path.join(config.get("paths", "quarterly_data_path"), "Section_6__Household.csv"))]


class ReadAllSections(ReadDataFrameTask):

    def requires(self):
        return AllSectionsExternal()


class ChrisCalcTask(Task):
    """
    This is a docstring for ChrisCalcTask
    """

    def requires(self):
        return ReadAllSections()

    def output(self):
        return ExpiringMemoryTarget(name='xyz_calculated_data', timeout=10)

    def run(self):
        """
        Calculate the things. Return dataframe with data inside. docstring for a method
        """
        data_dict = self.input().get()['clean_data']
        df = data_dict['xyz'].copy()

        self.output().put(df)

    def func_xyz(self, temp_df, xyz_q_list):
        """
        Translates and sums over proper values to calculate XYZ number score.
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
        return ExpiringMemoryTarget(name='aggregated_weighted_data', timeout=10)

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
        return ExpiringMemoryTarget(name='aggregated_unweighted_data', timeout=10)

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
        return ExpiringMemoryTarget(name='appended_data', timeout=10)

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
        return ExpiringMemoryTarget(name='data_confidence_intervals', timeout=10)

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