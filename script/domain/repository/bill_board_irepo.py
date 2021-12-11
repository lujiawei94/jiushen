from script.infrastructure.repository.ak_share_repo.bill_board_repo import BillBoardRepo

class BillBoardIrepo:


    @staticmethod
    def searchable_indicators_by_date(check_date):
        return BillBoardRepo().searchable_indicators_by_date(check_date=check_date)

    @staticmethod
    def lhb_ggtj_recently_by_days(check_date):
        return BillBoardRepo().lhb_ggtj_recently_by_days(5,check_date=check_date)

    @staticmethod
    def lhb_yytj_recently_by_days(check_date):
        return BillBoardRepo().lhb_yytj_recently_by_days(5,check_date=check_date)

    @staticmethod
    def lhb_jgzz_recently_by_days(check_date):
        return BillBoardRepo().lhb_jgzz_recently_by_days(5,check_date=check_date)

    @staticmethod
    def lhb_jgmx_recently(check_date):
        return BillBoardRepo().lhb_jgmx_recently(check_date=check_date)


    @staticmethod
    def get_bill_board_object(code_id):
        return BillBoardRepo()._aggregate_root_storage.get(code_id)


if __name__ == '__main__':
    res = BillBoardIrepo.lhb_jgzz_recently_by_days('2021-11-28')
    ...