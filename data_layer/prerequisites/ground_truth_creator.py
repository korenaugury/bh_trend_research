import os

import pandas as pd


class GroundTruthCreator:

    def __init__(self):
        self._relabelling_file_path = os.path.join(os.getcwd(), 'trend_relabelling.xlsx')
        self.ground_truth = None

    def create(self):
        melted_trend_df = self._get_melted_trend_sheet()
        for_review_df = self._add_additional_columns(self._get_for_review_sheet(), melted_trend_df)
        for_review_2_df = self._add_additional_columns(self._get_for_review_2_sheet(), melted_trend_df)

        for_review_df = for_review_df[~for_review_df['session_id'].isin(for_review_2_df['session_id'])]
        melted_trend_df = melted_trend_df[~melted_trend_df['session_id'].isin(for_review_df['session_id'])]
        melted_trend_df = melted_trend_df[~melted_trend_df['session_id'].isin(for_review_2_df['session_id'])]

        self.ground_truth = pd.concat([melted_trend_df, for_review_df, for_review_2_df])

    def save_locally(self):
        if self.ground_truth is None:
            self.create()
        self.ground_truth.to_csv(os.path.join(os.getcwd(), 'trend_cleaned_relabelling.csv'), index=False)

    def _get_melted_trend_sheet(self):
        columns = ['machine_id', 'session_id', 'orig_review', 'mhi_change', 'review', 'until', 'additional_info', 'features']
        df = pd.read_excel(self._relabelling_file_path, sheet_name='melted_trend_2021')
        df = pd.concat([df[~pd.isnull(df['for_review'])].copy(), df[~pd.isnull(df['for_review_2'])].copy()])
        df = df[['Machine_id', 'Session_id', 'Original review', 'Original mhi change', 'Review', 'Until', 'Additional info',
                 'Relevant symptoms (if true)']].copy()
        df.columns = columns
        return df

    def _get_for_review_sheet(self):
        columns = ['session_id', 'review', 'additional_info', 'features']
        df = pd.read_excel(self._relabelling_file_path, sheet_name='for review')
        df = df[['session_id', 'review', 'Additional info', 'feature']].copy()
        df.columns = columns
        df = df.dropna(how='all')
        return df

    def _get_for_review_2_sheet(self):
        columns = ['session_id', 'review', 'additional_info', 'features']
        df = pd.read_excel(self._relabelling_file_path, sheet_name='for review 2')
        df = df[['Session_id', 'Review', 'Additional info', 'Relevant symptoms (if true)']].copy()
        df.columns = columns
        df = df.dropna(how='all')
        return df

    @staticmethod
    def _add_additional_columns(for_rev, melted_trend):
        mt_to_join = melted_trend[['session_id', 'machine_id', 'orig_review', 'mhi_change', 'until']].copy()
        return for_rev.merge(mt_to_join, how='left', on='session_id')


