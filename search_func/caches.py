class Cache(object):
    def __int__(self, page_id_list: list):
        self.page_id_list = page_id_list
        self.re_rank_list = []

    def __call__(self, *args, **kwargs):
        return self.re_rank_list

    def previous_result(self):
        pass
