import datetime
from loguru import logger

statistics_map = {
    'total_match_time': 0,
    'same_country_match_pair_num': 0,
    'dif_gender_match_pair_num': 0,
    'successed_match_pair_num': 0,
    'score_inf': 0,
    'max_num_consecutive_fail': 0,
    'person_with_target_num': {0: 0, 1: 0, 2: 0, 3: 0, 'other': 0},
    'person_with_target_success_num': {0: 0, 1: 0, 2: 0, 3: 0, 'other': 0},
    'female_match_num': {0: 0, 1: 0, 2: 0, 3: 0, 'other': 0},
    'female_match_success_num': {0: 0, 1: 0, 2: 0, 3: 0, 'other': 0},
    'male_match_num': {0: 0, 1: 0, 2: 0, 3: 0, 'other': 0},
    'male_match_success_num': {0: 0, 1: 0, 2: 0, 3: 0, 'other': 0},
}


class Analysis:

    def __init__(self, log_path):
        self.statistics_map = statistics_map
        self.log_path = f"{log_path}/{(datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y_%m_%d_%H')}.log"

    def statistics(self, one_match_map):
        self.statistics_map['total_match_time'] += 1
        for user_id, info in one_match_map['user_map'].items():
            success = True if user_id in [j for i in one_match_map['match_res'] for j in i] else False
            failed = True if user_id in one_match_map['failed_list'] else False

            self.statistics_map['max_num_consecutive_fail'] = max(self.statistics_map['max_num_consecutive_fail'],
                                                                  info['num_consecutive_fail'])

            num_consecutive_fail = info['num_consecutive_fail'] if info['num_consecutive_fail'] <= 3 else 'other'

            if info['gender'] == 1:
                self.statistics_map['male_match_num'][num_consecutive_fail] += 1
                if success:
                    self.statistics_map['male_match_success_num'][num_consecutive_fail] += 1
            else:
                self.statistics_map['female_match_num'][num_consecutive_fail] += 1
                if success:
                    self.statistics_map['female_match_success_num'][num_consecutive_fail] += 1

            if info['targeted_match']:
                self.statistics_map['person_with_target_num'][num_consecutive_fail] += 1
                if success:
                    self.statistics_map['person_with_target_success_num'][num_consecutive_fail] += 1

        for success_pair in one_match_map['match_res']:
            self.statistics_map['successed_match_pair_num'] += 1
            if one_match_map['user_map'][success_pair[0]]['country'] == one_match_map['user_map'][success_pair[1]][
                'country']:
                self.statistics_map['same_country_match_pair_num'] += 1
            if one_match_map['user_map'][success_pair[0]]['gender'] == one_match_map['user_map'][success_pair[1]][
                'gender']:
                self.statistics_map['dif_gender_match_pair_num'] += 1

        if one_match_map['score_res'] == 'inf':
            self.statistics_map['score_inf'] += 1

    def start_statistics(self):
        with open(self.log_path) as f:

            one_match_map = {}
            while line := f.readline():
                if datetime.datetime.now().strftime('%Y-%m-%d') in line:
                    if one_match_map:
                        self.statistics(one_match_map)
                        one_match_map = {}
                else:
                    key, value = line.split(':', 1)
                    if key != 'group_id':
                        one_match_map[key] = eval(value, {'inf': 'inf'})
            logger.info(self.statistics_map)

            self.individual_matching_track()
            self.pair_match_track()

    def individual_matching_track(self):
        male_num = sum(self.statistics_map['male_match_num'].values())
        male_success = sum(self.statistics_map['male_match_success_num'].values())
        female_num = sum(self.statistics_map['female_match_num'].values())
        female_success = sum(self.statistics_map['female_match_success_num'].values())
        female_num_at_first = self.statistics_map['female_match_num'][0]
        female_success_at_first = self.statistics_map['female_match_success_num'][0]
        male_num_at_first = self.statistics_map['male_match_num'][0]
        male_success_at_first = self.statistics_map['male_match_success_num'][0]
        total_num = male_num + female_num
        total_num_success = male_success + female_success
        total_num_fail = total_num - total_num_success
        num_consecutive_fail_gt_3 = self.statistics_map['male_match_num']['other'] + \
                                    self.statistics_map['female_match_num']['other']
        num_with_target = sum(self.statistics_map['person_with_target_num'].values())
        num_with_target_success = sum(self.statistics_map['person_with_target_success_num'].values())
        num_with_target_fail_at_first = self.statistics_map['person_with_target_num'][0]
        num_with_target_success_fail_at_first = self.statistics_map['person_with_target_success_num'][0]

        fail_rate = round(total_num_fail / total_num, 2)  # 匹配失败的人数/总匹配人数
        match_fail_gt_3_rate = round(num_consecutive_fail_gt_3 / total_num, 2)  # num_consecutive_fail>3的数量/总匹配人数
        female_success_rate = round(female_success / female_num, 2)  # 女性匹配成功数/女性匹配总数
        male_success_rate = round(male_success / male_num, 2)  # 男性匹配成功数/男性匹配总数
        female_success_at_first_rate = round(female_success_at_first / female_num_at_first, 2)  # 女性首次匹配成功数/女性首次匹配总数
        male_success_at_first_rate = round(male_success_at_first / male_num_at_first, 2)  # 男性首次匹配成功数/男性匹配总数
        target_success_rate = round(num_with_target_success / num_with_target, 2)  # 有target成功匹配/ target总数
        target_success_at_first_rate = round(num_with_target_success_fail_at_first / num_with_target_fail_at_first,
                                             2)  # 有target，首次成功匹配/ target首次匹配总数
        max_num_consecutive_fail = round(self.statistics_map['max_num_consecutive_fail'], 2)  # 最大num_consecutive_fail

        logger.info(
            f'\nfail_rate: {fail_rate}\nmatch_fail_gt_3_rate: {match_fail_gt_3_rate}\nfemale_success_rate: {female_success_rate}\nmale_success_rate: {male_success_rate}\nfemale_success_at_first_rate: {female_success_at_first_rate}\nmale_success_at_first_rate: {male_success_at_first_rate}\ntarget_success_rate: {target_success_rate}\ntarget_success_at_first_rate: {target_success_at_first_rate}\nmax_num_consecutive_fail: {max_num_consecutive_fail}')

    def pair_match_track(self):
        successed_match_pair_num = self.statistics_map['successed_match_pair_num']
        same_country_match_pair_num = self.statistics_map['same_country_match_pair_num']
        dif_gender_match_pair_num = self.statistics_map['dif_gender_match_pair_num']
        random_times = self.statistics_map['score_inf']

        same_country_rate = round(same_country_match_pair_num / successed_match_pair_num, 2)  # 同国家的匹配/匹配成功的匹配对总数
        dif_gender_match_rate = round(dif_gender_match_pair_num / successed_match_pair_num, 2)  # 异性匹配对/匹配成功的匹配对总数
        random_match_rate = round(random_times / successed_match_pair_num, 2)  # random策略/总匹配次数

        logger.info(
            f'\nsame_country_rate: {same_country_rate}\ndif_gender_match_rate: {dif_gender_match_rate}\nrandom_match_rate: {random_match_rate}')


if __name__ == '__main__':
    Analysis('/opt/logs/Matches_hours_record').start_statistics()
