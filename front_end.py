import os
import sys
sys.path.append(os.getcwd())
from app_Lib import manage_directories as md, functions as funcs
import multiprocessing
from tkinter import *
from tkinter import messagebox, filedialog, ttk
import datetime as dt
import traceback
import time
import threading
import random
import profiling as prof


class TxtField:
    def __init__(self, frame, frame_width, label_txt, label_row, label_column, label_sticky, entry_row, entry_column, entry_sticky):
        label = Label(frame, text=label_txt)
        label.grid(row=label_row, column=label_column, sticky=label_sticky)

        self.entry_field = Entry(frame, textvariable=StringVar(), width=frame_width)
        self.entry_field.grid(row=entry_row, column=entry_column, sticky=entry_sticky)


class FrontEnd:
    def __init__(self):
        self.root = Tk()
        img_icon = PhotoImage(file=os.path.join(md.get_dirs()[0], 'script_icon.png'))
        self.root.tk.call('wm', 'iconphoto', self.root._w, img_icon)
        self.root.wm_title("Exploratory Data Analysis." + " | Build #1.1.1")
        self.root.resizable(width="false", height="false")

        self.msg_ready = "Ready"
        self.color_msg_ready = "green"
        self.msg_profiling = "In Progress... "
        self.color_msg_profiling = "blue"
        self.msg_done = "Done, Elapsed Time: "
        self.color_msg_done = "green"
        self.color_msg_done_with_error = "red"
        self.color_error_messager = "red"

        # "'source_engine', 'url', 'schema', 'query', 'host', 'port', 'database',
        # 'user', 'pw', 'read_file', 'sheet_name', 'csv_delimiter', 'save_result_to', 'data_set_name'"

        frame_row1 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row1.grid(column=0, row=1, sticky=W)

        ##############################  start buttons frame  ####################################
        frame_buttons = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_buttons.grid(column=1, row=0)
        self.profile_button = Button(frame_buttons, text="Profile", width=12, height=2, command=self.start_profiling)
        self.profile_button.grid(row=2, column=0)
        close_button = Button(frame_buttons, text="Exit", width=12, height=2, command=self.close)
        close_button.grid(row=3, column=0)
        ##############################  end buttons frame  ####################################

        ##############################  start user inputs frame  ####################################
        frame_db_inputs = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_db_inputs.grid(column=0, row=0, sticky="w")
        frame_db_inputs_width = 80# 84

        self.entry_field_source_engine = TxtField(frame_db_inputs, frame_db_inputs_width, "Source Engine", 0, 0, 'e', 0, 1, "w").entry_field
        self.entry_field_url = TxtField(frame_db_inputs, frame_db_inputs_width, "URL", 1, 0, 'e', 1, 1, "w").entry_field
        self.entry_field_host = TxtField(frame_db_inputs, frame_db_inputs_width, "Host", 4, 0, 'e', 4, 1, "w").entry_field
        self.entry_field_port = TxtField(frame_db_inputs, frame_db_inputs_width, "Port", 5, 0, 'e', 5, 1, "w").entry_field
        self.entry_field_database = TxtField(frame_db_inputs, frame_db_inputs_width, "Database", 6, 0, 'e', 6, 1, "w").entry_field
        self.entry_field_Schema = TxtField(frame_db_inputs, frame_db_inputs_width, "Schema", 7, 0, 'e', 7, 1, "w").entry_field
        self.entry_field_user = TxtField(frame_db_inputs, frame_db_inputs_width, "User", 8, 0, 'e', 8, 1, "w").entry_field
        self.entry_field_password = TxtField(frame_db_inputs, frame_db_inputs_width, "Password", 9, 0, 'e', 9, 1, "w").entry_field
        self.entry_field_save_report_to = TxtField(frame_db_inputs, frame_db_inputs_width, "Save Report To", 10, 0, 'e', 10, 1, "w").entry_field
        self.entry_field_dataset_name = TxtField(frame_db_inputs, frame_db_inputs_width, "Dataset Name", 11, 0, 'e', 11, 1, "w").entry_field
        self.entry_field_Query = TxtField(frame_db_inputs, frame_db_inputs_width, "Query", 12, 0, 'e', 12, 1, "w").entry_field
        ##############################  start user inputs frame  ####################################

        ##############################  start Footer  ####################################
        frame_footer_status_row2 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_footer_status_row2.grid(column=0, row=2, sticky=W + E)

        frame_footer_status_row2.grid_columnconfigure(0, weight=1, uniform="group1")
        frame_footer_status_row2.grid_columnconfigure(1, weight=1, uniform="group1")
        frame_footer_status_row2.grid_rowconfigure(0, weight=1)

        frame_footer_status_row2_l = Frame(frame_footer_status_row2, borderwidth="2", relief="ridge")
        frame_footer_status_row2_l.grid(column=0, row=3, sticky=W + E)

        frame_footer_status_row2_r = Frame(frame_footer_status_row2, borderwidth="2", relief="ridge")
        frame_footer_status_row2_r.grid(column=1, row=3, sticky=W + E)

        self.status_label_text = StringVar()
        self.status_label = Label(frame_footer_status_row2_l)
        self.status_label.grid(column=0, row=0, sticky=W)

        self.server_info_label_text = StringVar()
        self.server_info_label = Label(frame_footer_status_row2_r)
        self.server_info_label.grid(column=1, row=0, sticky=E)
        ##############################  end Footer  ####################################

        self.thread0 = DataProfilingThread(0, "Thread-0", self)
        self.thread0.start()

        self.root.mainloop()

    def change_status_label(self, msg, color):
        self.status_label_text.set(msg)
        self.status_label.config(fg=color, text=self.status_label_text.get())

    def change_server_info_label(self, msg, color):
        try:
            self.server_info_label_text.set(msg)
            self.server_info_label.config(fg=color, text=self.server_info_label_text.get())
        except RuntimeError:
            pass

    def pb(self, tasks, task_len):
        self.progress_var = IntVar()
        pb = ttk.Progressbar(self.root, orient="horizontal",
                             length=300, maximum=task_len - 1,
                             mode="determinate",
                             var=self.progress_var)
        pb.grid(row=3, column=1)

        for i, task in enumerate(tasks):
            self.progress_var.set(i)
            i += 1
            # time.sleep(1 / 60)
            # compute(task)
            self.root.update_idletasks()

    def enable_disable_fields(self, f_state):
        self.profile_button.config(state=f_state)

    def data_profile_thread(self):
        try:
            self.enable_disable_fields(DISABLED)
            self.dp.data_profile()
            self.enable_disable_fields(NORMAL)
            print("Total Elapsed time: ", self.dp.elapsed_time, "\n")
        except Exception as error:
            try:
                error_messager = self.dp.error_message
            except:
                error_messager = error
            self.change_status_label(error_messager, self.color_error_messager)
            self.profile_button.config(state=NORMAL)
            traceback.print_exc()

    def destroyer(self):
        self.root.quit()
        self.root.destroy()
        sys.exit()


    def close(self):
        # self.destroyer()
        self.root.protocol("WM_DELETE_WINDOW", self.destroyer())

    def start_profiling(self):
        self.dp = prof.DataProfiling(self.entry_field_source_engine.get(), self.entry_field_url.get(), self.entry_field_Schema.get(),self.entry_field_Query.get(),
                                     self.entry_field_host.get(), self.entry_field_port.get(), self.entry_field_database.get(), self.entry_field_user.get(),
                                     self.entry_field_password.get(), self.entry_field_save_report_to.get(), self.entry_field_dataset_name.get())

        thread1 = DataProfilingThread(1, "Thread-1", self)
        thread1.start()

        thread2 = DataProfilingThread(2, "Thread-2", self, thread1)
        thread2.start()

    def generating_indicator(self, thread):
        def r():
            return random.randint(0, 255)

        while thread.is_alive():
            elapsed_time = dt.datetime.now() - self.dp.start_time
            msg = self.msg_profiling + str(elapsed_time)
            color = '#%02X%02X%02X' % (r(),r(),r())
            self.change_status_label(msg, color)

        message = self.dp.error_message if self.dp.error_message != "" else self.msg_done + str(self.dp.elapsed_time)
        color = self.color_msg_done_with_error if self.dp.error_message != "" else self.color_msg_done
        self.change_status_label(message, color)

    def display_server_info(self, thread):
        color = "blue"
        while True:
            server_info = funcs.server_info()
            msg = "CPU " + str(server_info[0]) + "%" + " Memory " + str(server_info[1]) + "%"
            self.change_server_info_label(msg, color)


class DataProfilingThread(threading.Thread):
    def __init__(self,threadID ,name, front_end_c, thread=None):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.FrontEndC = front_end_c
        self.thread = thread
        self.daemon = True

    def run(self):
        if self.threadID == 1:
            self.FrontEndC.data_profile_thread()
        if self.threadID == 2:
            self.FrontEndC.generating_indicator(self.thread)
        if self.threadID == 0:
            self.FrontEndC.display_server_info(self.thread)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    FrontEnd()
