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


class FrontEnd:
    def __init__(self):
        self.root = Tk()
        img_icon = PhotoImage(file=os.path.join(md.get_dirs()[0], 'script_icon.png'))
        self.root.tk.call('wm', 'iconphoto', self.root._w, img_icon)
        self.root.wm_title("SMX Scripts Builder " + pm.ver_no)
        self.root.resizable(width="false", height="false")

        self.msg_no_config_file = "No Config File Found!"
        self.color_msg_no_config_file = "red"
        self.msg_ready = "Ready"
        self.color_msg_ready = "green"
        self.msg_generating = "In Progress... "
        self.color_msg_generating = "blue"
        self.msg_done = "Done, Elapsed Time: "
        self.color_msg_done = "green"
        self.color_msg_done_with_error = "red"
        self.color_error_messager = "red"

        frame_row1 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row1.grid(column=0, row=1, sticky=W)

        frame_row2 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row2.grid(column=0, row=2, sticky=W + E)

        frame_row2.grid_columnconfigure(0, weight=1, uniform="group1")
        frame_row2.grid_columnconfigure(1, weight=1, uniform="group1")
        frame_row2.grid_rowconfigure(0, weight=1)

        frame_row2_l = Frame(frame_row2, borderwidth="2", relief="ridge")
        frame_row2_l.grid(column=0, row=3, sticky=W + E)

        frame_row2_r = Frame(frame_row2, borderwidth="2", relief="ridge")
        frame_row2_r.grid(column=1, row=3, sticky=W + E)

        self.status_label_text = StringVar()
        self.status_label = Label(frame_row2_l)
        self.status_label.grid(column=0, row=0, sticky=W)

        self.server_info_label_text = StringVar()
        self.server_info_label = Label(frame_row2_r)
        self.server_info_label.grid(column=1, row=0, sticky=E)

        frame_buttons = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_buttons.grid(column=1, row=0)
        self.generate_button = Button(frame_buttons, text="Profile", width=12, height=2, command=self.start)
        self.generate_button.grid(row=2, column=0)
        close_button = Button(frame_buttons, text="Exit", width=12, height=2, command=self.close)
        close_button.grid(row=3, column=0)

        frame_database_inputs = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_database_inputs.grid(column=0, row=0, sticky="w")

        frame_config_file_values_entry_width = 84

        read_from_smx_label = Label(frame_database_inputs, text="SMXs Folder")
        read_from_smx_label.grid(row=0, column=0, sticky='e')

        self.text_field_read_from_smx = StringVar()
        self.entry_field_read_from_smx = Entry(frame_database_inputs, textvariable=self.text_field_read_from_smx, width=frame_config_file_values_entry_width)
        self.entry_field_read_from_smx.grid(row=0, column=1, sticky="w")

        output_path_label = Label(frame_database_inputs, text="Output Folder")
        output_path_label.grid(row=1, column=0, sticky='e')

        self.text_field_output_path = StringVar()
        self.entry_field_output_path = Entry(frame_database_inputs, textvariable=self.text_field_output_path, width=frame_config_file_values_entry_width)
        self.entry_field_output_path.grid(row=1, column=1, sticky="w")

        source_names_label = Label(frame_database_inputs, text="Sources")
        source_names_label.grid(row=2, column=0, sticky='e')

        self.text_field_source_names = StringVar()
        self.entry_field_source_names = Entry(frame_database_inputs, textvariable=self.text_field_source_names, width=frame_config_file_values_entry_width)
        self.entry_field_source_names.grid(row=2, column=1, sticky="w", columnspan=1)

        db_prefix_label = Label(frame_database_inputs, text="DB Prefix")
        db_prefix_label.grid(row=3, column=0, sticky='e')

        self.text_db_prefix = StringVar()
        self.entry_db_prefix = Entry(frame_database_inputs, textvariable=self.text_db_prefix, width=frame_config_file_values_entry_width)
        self.entry_db_prefix.grid(row=3, column=1, sticky="w", columnspan=1)

        self.populate_config_file_values()

        thread0 = GenerateScriptsThread(0, "Thread-0", self)
        thread0.start()

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
        self.generate_button.config(state=f_state)

    def generate_scripts_thread(self):
        try:
            config_file_path = self.config_file_entry_txt.get()
            x = open(config_file_path)
            try:
                self.enable_disable_fields(DISABLED)
                self.g.generate_scripts()
                self.enable_disable_fields(NORMAL)
                print("Total Elapsed time: ", self.g.elapsed_time, "\n")
            except Exception as error:
                try:
                    error_messager = self.g.error_message
                except:
                    error_messager = error
                self.change_status_label(error_messager, self.color_error_messager)
                self.generate_button.config(state=NORMAL)
                self.config_file_entry.config(state=NORMAL)
                traceback.print_exc()
        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)

    def destroyer(self):
        self.root.quit()
        self.root.destroy()
        sys.exit()

    def close(self):
        self.root.protocol("WM_DELETE_WINDOW", self.destroyer())

    def start(self):

        self.refresh_config_file_values()
        self.g = gs.GenerateScripts(None, self.config_file_values)

        thread1 = GenerateScriptsThread(1, "Thread-1", self)
        thread1.start()

        thread2 = GenerateScriptsThread(2, "Thread-2", self, thread1)
        thread2.start()

    def generating_indicator(self, thread):
        def r():
            return random.randint(0, 255)

        while thread.is_alive():
            elapsed_time = dt.datetime.now() - self.g.start_time
            msg = self.msg_generating + str(elapsed_time)
            # color_list = ["white", "black", "red", "green", "blue", "cyan", "yellow", "magenta"]
            # color = random.choice(color_list)
            color = '#%02X%02X%02X' % (r(),r(),r())
            self.change_status_label(msg, color)

        message = self.g.error_message if self.g.error_message != "" else self.msg_done + str(self.g.elapsed_time)
        color = self.color_msg_done_with_error if self.g.error_message != "" else self.color_msg_done
        self.change_status_label(message, color)

    def display_server_info(self, thread):
        color = "blue"
        while True:
            server_info = funcs.server_info()
            msg = "CPU " + str(server_info[0]) + "%" + " Memory " + str(server_info[1]) + "%"
            self.change_server_info_label(msg, color)


class GenerateScriptsThread(threading.Thread):
    def __init__(self,threadID ,name, front_end_c, thread=None):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.FrontEndC = front_end_c
        self.thread = thread

    def run(self):
        if self.threadID == 1:
            self.FrontEndC.generate_scripts_thread()
        if self.threadID == 2:
            self.FrontEndC.generating_indicator(self.thread)
        if self.threadID == 0:
            self.FrontEndC.display_server_info(self.thread)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    FrontEnd()
