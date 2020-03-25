import curses, uuid, threading
from contextlib import nullcontext
import unicodedata


class Display():
    lock = threading.Lock()

    def __init__(self):
        self.stdscr = None
        self.pages = {}
        self.page = 0
        self.max_page = 0
        self.lines = []
        self.offsets = {}
        self.refresh = {}
        self.id_to_page = {}
        self.height = None
        self.width = None
        self._current_page = -1
        self._current_offset = -1
        self.render_toppage = None
        self.update_toppage, _ = self.add_window(self.render_toppage_wrap)

    def render_toppage_wrap(self):
        if self.render_toppage:
            return self.render_toppage()
        return self.default_render_toppage()

    def default_render_toppage(self):
        return ''

    def add_window(self, reload_func, lock=None):
        if lock is None:
            lock = self.lock
        with lock:
            id = str(uuid.uuid4())
            page = self.max_page
            self.max_page += 1
            self.pages[page] = reload_func
            self.offsets[page] = 0
            self.id_to_page[id] = page

            def refresh():
                self.refresh[page] = True

        return refresh, id

    def delete_page(self, id=None, lock=None):
        if lock is None:
            lock = self.lock
        with lock:
            if id is not None:
                page = self.id_to_page[id]
            else:
                page = self.page
            if page == 0:
                return
            new_pages = {}
            new_offsets = {}
            for key, val in self.pages.items():
                if key > page:
                    key -= 1
                new_pages[key] = val
                new_offsets[key] = self.offsets[key]
            self.pages = new_pages
            self.offsets = new_offsets
            self.max_page -= 1
            if self.max_page == 0:
                self.add_window(self.render_toppage, lock=nullcontext)
            if self.page >= self.max_page:
                self.page -= 1
            for p in self.pages.keys():
                self.refresh[p] = True

    def change_page(self, page=None, offset=None, lock=None):
        if lock is None:
            lock = self.lock
        with lock:
            if page is None:
                page = self.page
            if offset is not None:
                page += offset
            while page < 0:
                page += len(self.pages)
            page = page % len(self.pages)
            self.page = page

    def _input(self):
        ch = self.stdscr.getch()
        while ch >= 0:
            if ch == curses.KEY_UP:
                self.offsets[self.page] = max(0, self.offsets[self.page] - 1)
            elif ch == curses.KEY_DOWN:
                self.offsets[self.page] = min(len(self.lines), self.offsets[self.page] + 1)
            elif ch == curses.KEY_LEFT:
                self.page = self.page - 1 if self.page > 0 else (self.max_page - 1)
            elif ch == curses.KEY_RIGHT:
                self.page = self.page + 1 if self.page < self.max_page - 1 else 0
            elif ch == curses.KEY_PPAGE:
                self.offsets[self.page] = max(0, self.offsets[self.page] - (self.height - 1))
            elif ch == curses.KEY_NPAGE:
                self.offsets[self.page] = min(len(self.lines), self.offsets[self.page] + (self.height - 1))
            ch = self.stdscr.getch()

    def render(self):
        with self.lock:
            self._input()
            height, width = self.stdscr.getmaxyx()
            should_update = self._current_page != self.page or height != self.height or width != self.width or self.page in self.refresh
            should_render = should_update or self._current_offset != self.offsets[self.page]
            self.height = height
            self.width = width

            if should_update:
                content = self.pages[self.page]()
                lines = []
                for line in content.split('\n'):
                    if len(line) == 0:
                        line = ' '
                    while len(line) > 0:
                        sum = 0
                        for i in range(len(line)):
                            sum += 2 if unicodedata.east_asian_width(line[i]) in ['F', 'W', 'A'] else 1
                            if sum > width:
                                i -= 1
                                break
                        if sum > width:
                            lines.append(line[:i])
                            line = line[i + 1:]
                        else:
                            lines.append(line)
                            break

                self.lines = lines
                self._current_page = self.page
                if self.page in self.refresh:
                    del self.refresh[self.page]

            if should_render:
                self._current_offset = self.offsets[self.page]
                self.stdscr.erase()
                self.stdscr.move(0, 0)
                self.stdscr.addnstr(
                    'Page {} / {}, Offset {} / {}'.format(self.page + 1, self.max_page, self.offsets[self.page], len(self.lines)),
                    self.width,
                )
                for y in range(min(self.height - 1, len(self.lines) - self.offsets[self.page])):
                    self.stdscr.move(y + 1, 0)
                    line = self.lines[y + self.offsets[self.page]]
                    try:
                        self.stdscr.addnstr(line, min(len(line), self.width))
                    except curses.error:
                        ...
                self.stdscr.refresh()

    def __enter__(self):
        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.stdscr.keypad(True)
        self.stdscr.nodelay(True)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        curses.nocbreak()
        self.stdscr.keypad(False)
        curses.echo()
        curses.endwin()


if __name__ == '__main__':
    import time
    with Display() as display:
        message = ''
        update_func, id = display.add_window(lambda: message)
        display.add_window(lambda: 'a')
        display.add_window(lambda: 'b')
        display.add_window(lambda: 'c')
        while True:
            time.sleep(0.1)
            message += 'asdfasdf\n'
            update_func
            display.render()
