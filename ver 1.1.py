import sys
import os
import csv
import traceback
import uuid # Để tạo ID mạng duy nhất
from datetime import datetime

# --- Kiểm tra và Nhập Thư viện ---
try:
    import cantools
except ImportError:
    print("Lỗi: Thư viện 'cantools' chưa được cài đặt.")
    print("Vui lòng cài đặt bằng lệnh: pip install cantools")
    sys.exit(1)

try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QAction, QFileDialog, QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
        QStatusBar, QMessageBox, QSplitter, QHeaderView, QLabel, QMenuBar,
        QTabWidget, QPushButton, QLineEdit, QStackedWidget # Thêm QTabWidget, QPushButton, QLineEdit
    )
    from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject, QSize, QTimer # Thêm QTimer cho ví dụ cập nhật
    from PyQt5.QtGui import QIcon, QFont
except ImportError:
    print("Lỗi: Thư viện 'PyQt5' chưa được cài đặt.")
    print("Vui lòng cài đặt bằng lệnh: pip install PyQt5")
    sys.exit(1)

# Tùy chọn: Thư viện đồ thị
try:
    import pyqtgraph as pg
    PYQTGRAPH_AVAILABLE = True
except ImportError:
    PYQTGRAPH_AVAILABLE = False
    print("Cảnh báo: Thư viện 'pyqtgraph' không có sẵn. Tab đồ thị sẽ bị vô hiệu hóa.")
    print("Cài đặt bằng: pip install pyqtgraph")

# --- Worker Threads (Tương tự ví dụ trước, được điều chỉnh một chút) ---

class DbcLoadingWorker(QThread):
    finished = pyqtSignal(str, object, str) # Thêm network_id vào signal
    progress = pyqtSignal(str, str)        # Thêm network_id

    def __init__(self, network_id, file_path):
        super().__init__()
        self.network_id = network_id
        self.file_path = file_path

    def run(self):
        try:
            self.progress.emit(self.network_id, f"Phân tích DBC: {os.path.basename(self.file_path)}...")
            db = None
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252']
            for enc in encodings_to_try:
                try:
                    db = cantools.db.load_file(self.file_path, strict=False, encoding=enc)
                    self.progress.emit(self.network_id, f"Đọc DBC thành công với encoding '{enc}'.")
                    break # Thoát vòng lặp nếu thành công
                except UnicodeDecodeError:
                    continue # Thử encoding tiếp theo
                except Exception as inner_e: # Bắt lỗi khác từ cantools.load_file
                     # Nếu lỗi không phải do encoding, ném ra ngoài
                     if "encoding" not in str(inner_e).lower():
                          raise inner_e
                     continue # Thử encoding tiếp theo nếu liên quan đến encoding


            if db is None:
                 raise ValueError(f"Không thể đọc file DBC bằng các encoding đã thử: {', '.join(encodings_to_try)}")

            self.finished.emit(self.network_id, db, self.file_path)
        except FileNotFoundError:
            self.finished.emit(self.network_id, None, f"Lỗi: Không tìm thấy file DBC '{self.file_path}'")
        except Exception as e:
            error_details = traceback.format_exc()
            self.finished.emit(self.network_id, None, f"Lỗi đọc DBC:\n{e}\n\nChi tiết:\n{error_details}")

class TraceLoadingWorker(QThread):
    finished = pyqtSignal(str, list, dict, str) # network_id, trace_data, signal_timeseries, file_path / or error
    progress = pyqtSignal(str, str)          # network_id, message
    progress_percent = pyqtSignal(str, int)  # network_id, percent

    def __init__(self, network_id, file_path, db=None): # Nhận cả db để giải mã nếu có
        super().__init__()
        self.network_id = network_id
        self.file_path = file_path
        self.db = db # Database object để giải mã

    def run(self):
        trace_data = []
        signal_timeseries = {} # { 'SignalName': ([timestamps], [values]) }
        message_counter = 0
        decoded_counter = 0
        error_counter = 0
        start_time = datetime.now()

        try:
            # Ước tính số dòng
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f_count:
                    total_lines = sum(1 for _ in f_count)
            except Exception:
                total_lines = 0

            self.progress.emit(self.network_id, f"Đọc Trace: {os.path.basename(self.file_path)}...")
            with open(self.file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile) # Dùng csv.reader để xử lý tốt hơn
                # Bỏ qua header nếu muốn: next(reader, None)
                line_num = 0
                for row in reader:
                    line_num += 1
                    if line_num % 5000 == 0: # Cập nhật tiến trình
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = int(line_num / elapsed) if elapsed > 0 else 0
                        self.progress.emit(self.network_id, f"Đọc dòng {line_num} ({rate} dòng/s)...")
                        if total_lines > 0:
                            percent = int((line_num / total_lines) * 100)
                            self.progress_percent.emit(self.network_id, percent)

                    if len(row) < 4:
                        if any(field.strip() for field in row): # Bỏ qua nếu không phải dòng trống
                            # print(f"Cảnh báo (NetID: {self.network_id}): Bỏ qua dòng {line_num}, thiếu cột: {row}")
                            error_counter += 1
                        continue

                    timestamp_str, id_str, dlc_str, data_str = [field.strip() for field in row[:4]]

                    # Xử lý ID
                    try:
                        can_id = int(id_str, 16) if id_str.lower().startswith('0x') else int(id_str)
                        id_hex = f"{can_id:X}"
                    except ValueError:
                        # print(f"Cảnh báo (NetID: {self.network_id}): Dòng {line_num}, ID không hợp lệ: '{id_str}'")
                        error_counter += 1
                        continue

                    # Xử lý DLC
                    try:
                        dlc = int(dlc_str)
                        if not (0 <= dlc <= 64): # Cho phép CAN FD
                           raise ValueError("DLC ngoài khoảng hợp lệ (0-64)")
                    except ValueError:
                        # print(f"Cảnh báo (NetID: {self.network_id}): Dòng {line_num}, DLC không hợp lệ: '{dlc_str}'")
                        error_counter += 1
                        continue

                    # Xử lý Data
                    data_hex = "".join(data_str.split()).upper()
                    try:
                        data_bytes = bytes.fromhex(data_hex)
                        # Kiểm tra DLC và độ dài data (tùy chọn, có thể linh hoạt)
                        # expected_len = cantools.database.can.message.Message.dlc_to_length(dlc) # Cần import Message
                        # if len(data_bytes) != expected_len:
                        #      print(f"Cảnh báo (NetID: {self.network_id}): Dòng {line_num}, Data length {len(data_bytes)} không khớp DLC {dlc} (expected {expected_len})")
                        #      pass # Vẫn xử lý nếu muốn
                    except ValueError:
                        # print(f"Cảnh báo (NetID: {self.network_id}): Dòng {line_num}, Data hex không hợp lệ: '{data_str}'")
                        error_counter += 1
                        continue

                    trace_data.append([timestamp_str, id_hex, str(dlc), data_hex])
                    message_counter += 1

                    # --- Giải mã và tạo timeseries nếu có DBC ---
                    if self.db:
                        try:
                            message = self.db.get_message_by_frame_id(can_id)
                            decoded_signals = message.decode(data_bytes, decode_choices=False, allow_truncated=True)
                            decoded_counter += 1
                            # Giả sử timestamp là số float hoặc có thể chuyển đổi
                            try:
                                ts_float = float(timestamp_str)
                            except ValueError:
                                # Nếu timestamp không phải số, dùng index hoặc bỏ qua cho timeseries
                                ts_float = line_num # Dùng số dòng làm "thời gian" tạm thời

                            for sig_name, sig_value in decoded_signals.items():
                                if sig_name not in signal_timeseries:
                                    signal_timeseries[sig_name] = ([], []) # ([timestamps], [values])
                                signal_timeseries[sig_name][0].append(ts_float)
                                signal_timeseries[sig_name][1].append(sig_value)

                        except KeyError:
                            pass # ID không có trong DBC, không giải mã được
                        except Exception as decode_err:
                            # print(f"Lỗi giải mã dòng {line_num} (ID: {id_hex}): {decode_err}")
                            error_counter += 1 # Tăng lỗi nếu giải mã thất bại

            self.progress.emit(self.network_id, f"Đọc Trace hoàn tất ({message_counter} msgs). Giải mã: {decoded_counter}. Lỗi: {error_counter}")
            self.progress_percent.emit(self.network_id, 100)
            self.finished.emit(self.network_id, trace_data, signal_timeseries, self.file_path)

        except FileNotFoundError:
            self.finished.emit(self.network_id, None, {}, f"Lỗi: Không tìm thấy file trace '{self.file_path}'")
        except Exception as e:
            error_details = traceback.format_exc()
            self.finished.emit(self.network_id, None, {}, f"Lỗi đọc Trace:\n{e}\n\nChi tiết:\n{error_details}")

# --- Placeholder cho Logging Worker ---
class LoggingWorker(QThread):
    error = pyqtSignal(str, str) # network_id, error_message
    status = pyqtSignal(str, str) # network_id, status_message

    def __init__(self, network_id, log_file_path):
        super().__init__()
        self.network_id = network_id
        self.log_file_path = log_file_path
        self._is_running = False
        self.message_queue = [] # Đơn giản hóa: dùng list thay vì Queue thực sự
        self.writer = None
        self.file = None

    def run(self):
        self._is_running = True
        try:
            self.status.emit(self.network_id, f"Bắt đầu ghi log vào: {os.path.basename(self.log_file_path)}")
            # Mở file và tạo CSV writer
            self.file = open(self.log_file_path, 'w', newline='', encoding='utf-8')
            self.writer = csv.writer(self.file)
            self.writer.writerow(['Timestamp', 'ID_Hex', 'DLC', 'Data_Hex']) # Viết header

            while self._is_running:
                # Xử lý message trong queue (trong ví dụ này, queue được thêm từ bên ngoài)
                while self.message_queue:
                    message = self.message_queue.pop(0)
                    self.writer.writerow(message) # Ghi vào file
                self.msleep(50) # Ngủ một chút để tránh CPU load cao

            self.file.flush() # Đảm bảo dữ liệu được ghi hết
            self.status.emit(self.network_id, "Ghi log đã dừng.")

        except Exception as e:
            self.error.emit(self.network_id, f"Lỗi ghi log: {e}")
            self.status.emit(self.network_id, "Lỗi ghi log.")
        finally:
            if self.file:
                self.file.close()
                self.file = None
                self.writer = None
            self._is_running = False # Đảm bảo cờ được đặt lại

    def stop(self):
        self.status.emit(self.network_id, "Đang dừng ghi log...")
        self._is_running = False

    def add_message_to_queue(self, message_list):
         # Cần cơ chế thread-safe nếu nhiều luồng cùng add (dùng QMutex hoặc Queue)
         # Ở đây giả sử chỉ có luồng chính add vào
         if self._is_running and self.writer:
              self.message_queue.append(message_list)


# --- Các Widget cho từng Tab ---

class BaseNetworkTab(QWidget):
    """Lớp cơ sở cho các tab, chứa tham chiếu đến network_data hiện tại."""
    loadDbcRequested = pyqtSignal(str) # network_id
    loadTraceRequested = pyqtSignal(str) # network_id
    selectLogFileRequested = pyqtSignal(str) # network_id
    toggleLoggingRequested = pyqtSignal(str) # network_id

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_network_id = None
        self.network_data = {} # Giữ một bản copy cục bộ để tham chiếu

    def update_content(self, network_id, network_data):
        """Cập nhật nội dung của tab dựa trên network được chọn."""
        self.current_network_id = network_id
        self.network_data = network_data # Lưu lại tham chiếu hoặc copy nông
        # Các lớp con sẽ override phương thức này
        # print(f"Tab {self.__class__.__name__} updated for network: {network_id} - Name: {network_data.get('name', 'N/A')}")

class DbcConfigTab(BaseNetworkTab):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5) # Giảm margin

        # Nhãn hiển thị đường dẫn DBC
        self.dbcPathLabel = QLabel("File DBC: Chưa tải")
        self.dbcPathLabel.setWordWrap(True)
        layout.addWidget(self.dbcPathLabel)

        # Nút tải DBC
        self.loadDbcButton = QPushButton(QIcon.fromTheme("document-open"), "Tải / Thay đổi DBC...")
        self.loadDbcButton.clicked.connect(self._request_load_dbc)
        layout.addWidget(self.loadDbcButton)

        # Cây hiển thị cấu trúc DBC
        self.dbcStructureTree = QTreeWidget()
        self._setup_dbc_tree_widget()
        layout.addWidget(self.dbcStructureTree)

    def _setup_dbc_tree_widget(self):
        headers = [
            "Tên / Mô tả", "ID (Hex)", "DLC", "Sender(s)", "Start Bit", "Length",
            "Byte Order", "Type", "Factor", "Offset", "Unit", "Receivers", "Comment"
        ] # Rút gọn bớt cột so với ví dụ trước
        self.dbcStructureTree.setColumnCount(len(headers))
        self.dbcStructureTree.setHeaderLabels(headers)
        self.dbcStructureTree.header().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.dbcStructureTree.header().setSectionResizeMode(0, QHeaderView.Stretch)

    def _request_load_dbc(self):
        if self.current_network_id:
            self.loadDbcRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data):
        super().update_content(network_id, network_data)
        dbc_path = network_data.get('dbc_path', None)
        db = network_data.get('db', None)

        if dbc_path:
            self.dbcPathLabel.setText(f"File DBC: {dbc_path}")
        else:
            self.dbcPathLabel.setText("File DBC: Chưa tải")

        self.dbcStructureTree.clear()
        if db:
            self.populate_dbc_tree(db)

    def populate_dbc_tree(self, db):
        # (Tái sử dụng logic populate tree từ ví dụ trước - rút gọn ở đây)
        # Nhóm theo Node -> Message -> Signal
        # Lấy Nodes
        node_items = {}
        for node in sorted(db.nodes, key=lambda n: n.name):
            node_item = QTreeWidgetItem(self.dbcStructureTree, [node.name])
            node_items[node.name] = node_item
            if node.comment: node_item.setText(12, node.comment) # Cột comment

        # Phân loại messages
        no_sender_item = None
        sorted_messages = sorted(db.messages, key=lambda m: m.frame_id)
        for msg in sorted_messages:
             parent_item = None
             senders_str = ", ".join(sorted(msg.senders)) if msg.senders else "N/A"
             if msg.senders:
                 first_sender = sorted(msg.senders)[0]
                 if first_sender in node_items: parent_item = node_items[first_sender]
             if not parent_item: # Nếu ko có sender hoặc sender ko trong Nodes
                  if not no_sender_item:
                      no_sender_item = QTreeWidgetItem(self.dbcStructureTree, ["[Không rõ Sender hoặc Sender không định nghĩa]"])
                      self.dbcStructureTree.insertTopLevelItem(0, no_sender_item)
                  parent_item = no_sender_item

             # Add Message Item
             msg_data = [
                f"{msg.name}", f"0x{msg.frame_id:X}", str(msg.length), senders_str,
                "", "", "", "", "", "", "", "", # Signal cols empty
                msg.comment if msg.comment else ""
             ]
             message_item = QTreeWidgetItem(parent_item, msg_data)

             # Add Signal Items
             for sig in sorted(msg.signals, key=lambda s: s.start):
                  sig_data = [
                      f"  └─ {sig.name}", "", "", "", # Msg cols empty
                      str(sig.start), str(sig.length),
                      "Little" if sig.byte_order == 'little_endian' else "Big",
                      "Signed" if sig.is_signed else "Unsigned",
                      f"{sig.scale:.6g}", f"{sig.offset:.6g}",
                      sig.unit if sig.unit else "",
                      ", ".join(sorted(sig.receivers)) if sig.receivers else "",
                      sig.comment if sig.comment else ""
                  ]
                  QTreeWidgetItem(message_item, sig_data)

        # Expand Nodes
        for item in node_items.values(): item.setExpanded(True)
        if no_sender_item: no_sender_item.setExpanded(True)
        # Resize columns
        for i in range(1, self.dbcStructureTree.columnCount()):
             self.dbcStructureTree.resizeColumnToContents(i)


class TraceMessagesTab(BaseNetworkTab):
    signalValueUpdate = pyqtSignal(str, str, object, object) # net_id, sig_name, value, timestamp_obj

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        # Hàng điều khiển trên cùng
        control_layout = QHBoxLayout()
        self.loadTraceButton = QPushButton(QIcon.fromTheme("document-open"), "Tải File Trace (CSV)...")
        self.loadTraceButton.clicked.connect(self._request_load_trace)
        self.tracePathLabel = QLabel("Trace: Chưa tải") # Để hiển thị tóm tắt trace
        self.tracePathLabel.setWordWrap(True)
        control_layout.addWidget(self.loadTraceButton)
        control_layout.addWidget(self.tracePathLabel, 1) # Cho label co giãn
        layout.addLayout(control_layout)

        # Bảng hiển thị trace
        self.traceTable = QTableWidget()
        self._setup_trace_table()
        layout.addWidget(self.traceTable)

    def _setup_trace_table(self):
        self.traceTable.setColumnCount(5)
        headers = ["Timestamp", "ID (Hex)", "DLC", "Data (Hex)", "Decoded Signals"]
        self.traceTable.setHorizontalHeaderLabels(headers)
        self.traceTable.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.traceTable.horizontalHeader().setStretchLastSection(True)
        self.traceTable.setEditTriggers(QTableWidget.NoEditTriggers)
        self.traceTable.setSelectionBehavior(QTableWidget.SelectRows)
        self.traceTable.setAlternatingRowColors(True)
        # Tăng tốc độ hiển thị bảng lớn
        self.traceTable.setWordWrap(False)
        self.traceTable.setVerticalScrollMode(QTableWidget.ScrollPerPixel)
        self.traceTable.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)

    def _request_load_trace(self):
        if self.current_network_id:
            self.loadTraceRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data):
        super().update_content(network_id, network_data)
        trace_path = network_data.get('trace_path', None)
        trace_data = network_data.get('trace_data', [])
        db = network_data.get('db', None) # Lấy db để giải mã

        if trace_path:
             self.tracePathLabel.setText(f"Trace: {os.path.basename(trace_path)} ({len(trace_data)} msgs)")
        else:
             self.tracePathLabel.setText("Trace: Chưa tải")

        self.populate_trace_table(trace_data, db)

    def populate_trace_table(self, trace_data, db):
        self.traceTable.setRowCount(0)
        if not trace_data:
            return

        self.traceTable.setUpdatesEnabled(False) # Tắt update để tăng tốc
        self.traceTable.setRowCount(len(trace_data))

        last_decoded_values = {} # Lưu giá trị giải mã cuối cùng cho signal data tab

        for row_idx, row_data in enumerate(trace_data):
            timestamp, id_hex, dlc, data_hex = row_data

            item_ts = QTableWidgetItem(timestamp)
            item_id = QTableWidgetItem(id_hex)
            item_dlc = QTableWidgetItem(dlc)
            item_data = QTableWidgetItem(data_hex)
            item_decoded = QTableWidgetItem("") # Reset cột giải mã

            item_id.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            item_dlc.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            # item_data.setFont(QFont("Consolas", 9)) # Font monospace cho data

            self.traceTable.setItem(row_idx, 0, item_ts)
            self.traceTable.setItem(row_idx, 1, item_id)
            self.traceTable.setItem(row_idx, 2, item_dlc)
            self.traceTable.setItem(row_idx, 3, item_data)
            self.traceTable.setItem(row_idx, 4, item_decoded)

            # --- Giải mã nếu có DBC ---
            decoded_str = ""
            if db:
                try:
                    can_id = int(id_hex, 16)
                    message = db.get_message_by_frame_id(can_id)
                    data_bytes = bytes.fromhex(data_hex)
                    decoded_signals = message.decode(data_bytes, decode_choices=False, allow_truncated=True)
                    decoded_str = "; ".join([f"{name}={val:.4g}" for name, val in decoded_signals.items()])

                    # Phát tín hiệu cập nhật giá trị signal cho tab khác
                    ts_obj = timestamp # Giữ nguyên dạng string hoặc parse nếu cần
                    for sig_name, sig_value in decoded_signals.items():
                        self.signalValueUpdate.emit(self.current_network_id, sig_name, sig_value, ts_obj)
                        last_decoded_values[sig_name] = (sig_value, ts_obj) # Lưu lại mới nhất

                except KeyError:
                    decoded_str = "(ID không có trong DBC)"
                except ValueError:
                    decoded_str = "(Lỗi data hex)"
                except Exception as e:
                    decoded_str = f"(Lỗi decode: {type(e).__name__})"

                if decoded_str:
                    item_decoded.setText(decoded_str)
                    item_decoded.setToolTip(decoded_str) # Tooltip hữu ích khi cột hẹp

        self.traceTable.setUpdatesEnabled(True)
        # Tự động resize cột lần đầu, sau đó để user chỉnh
        # if not hasattr(self, '_columns_resized'):
        #      self.traceTable.resizeColumnsToContents()
        #      self._columns_resized = True

        print(f"Bảng trace cho net {self.current_network_id} cập nhật. Các giá trị signal cuối: {len(last_decoded_values)} signals")


class SignalDataTab(BaseNetworkTab):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        # Tùy chọn: Thêm Filter
        # filter_layout = QHBoxLayout()
        # filter_layout.addWidget(QLabel("Lọc tín hiệu:"))
        # self.filterEdit = QLineEdit()
        # self.filterEdit.setPlaceholderText("Nhập tên tín hiệu...")
        # self.filterEdit.textChanged.connect(self._apply_filter)
        # filter_layout.addWidget(self.filterEdit)
        # layout.addLayout(filter_layout)

        self.signalTable = QTableWidget()
        self._setup_signal_table()
        layout.addWidget(self.signalTable)

        self.latest_signal_values = {} # { 'SignalName': (value, timestamp) }

    def _setup_signal_table(self):
        self.signalTable.setColumnCount(4)
        headers = ["Tên Tín hiệu", "Giá trị Hiện tại", "Đơn vị", "Timestamp Giá trị"]
        self.signalTable.setHorizontalHeaderLabels(headers)
        self.signalTable.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.signalTable.horizontalHeader().setStretchLastSection(False) # Timestamp ko cần quá rộng
        self.signalTable.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch) # Kéo giãn cột tên
        self.signalTable.setEditTriggers(QTableWidget.NoEditTriggers)
        self.signalTable.setSelectionBehavior(QTableWidget.SelectRows)
        self.signalTable.setAlternatingRowColors(True)
        self.signalTable.setWordWrap(False)

    def update_content(self, network_id, network_data):
        super().update_content(network_id, network_data)
        db = network_data.get('db', None)
        self.latest_signal_values = network_data.get('latest_signal_values', {}) # Lấy giá trị mới nhất từ data chính

        self.populate_signal_list(db)

    def populate_signal_list(self, db):
        self.signalTable.setRowCount(0)
        if not db:
            return

        signals = []
        for msg in db.messages:
            for sig in msg.signals:
                # Tạo một định danh duy nhất nếu tên bị trùng (tên + tên message)
                # unique_sig_name = f"{sig.name}_{msg.name}" # Hoặc chỉ cần tên signal
                signals.append(sig)

        signals.sort(key=lambda s: s.name) # Sắp xếp theo tên

        self.signalTable.setUpdatesEnabled(False)
        self.signalTable.setRowCount(len(signals))
        self.signal_row_map = {} # Map tên signal -> chỉ số hàng

        for row_idx, sig in enumerate(signals):
            sig_name = sig.name
            self.signal_row_map[sig_name] = row_idx

            item_name = QTableWidgetItem(sig_name)
            item_value = QTableWidgetItem("") # Giá trị ban đầu trống
            item_unit = QTableWidgetItem(sig.unit if sig.unit else "")
            item_ts = QTableWidgetItem("")

            # Lấy giá trị mới nhất nếu có
            if sig_name in self.latest_signal_values:
                value, timestamp = self.latest_signal_values[sig_name]
                value_str = f"{value:.4g}" if isinstance(value, (float, int)) else str(value)
                item_value.setText(value_str)
                item_ts.setText(str(timestamp)) # Timestamp giữ nguyên dạng từ trace

            self.signalTable.setItem(row_idx, 0, item_name)
            self.signalTable.setItem(row_idx, 1, item_value)
            self.signalTable.setItem(row_idx, 2, item_unit)
            self.signalTable.setItem(row_idx, 3, item_ts)

        self.signalTable.setUpdatesEnabled(True)
        # self.signalTable.resizeColumnsToContents() # Chỉ resize lần đầu nếu cần

    def update_signal_value(self, signal_name, value, timestamp):
        """Slot để cập nhật một giá trị signal trong bảng."""
        if signal_name in self.signal_row_map:
            row_idx = self.signal_row_map[signal_name]
            value_str = f"{value:.4g}" if isinstance(value, (float, int)) else str(value)
            ts_str = str(timestamp)

            # Cập nhật item nếu nó tồn tại
            value_item = self.signalTable.item(row_idx, 1)
            ts_item = self.signalTable.item(row_idx, 3)

            if value_item:
                 value_item.setText(value_str)
            else: # Nếu item chưa có (ít xảy ra), tạo mới
                 self.signalTable.setItem(row_idx, 1, QTableWidgetItem(value_str))

            if ts_item:
                 ts_item.setText(ts_str)
            else:
                 self.signalTable.setItem(row_idx, 3, QTableWidgetItem(ts_str))
            # print(f"Updated signal {signal_name} in table") # Debug
        # else:
            # print(f"Signal {signal_name} not found in table map") # Debug

class GraphingTab(BaseNetworkTab):
     def __init__(self, parent=None):
          super().__init__(parent)
          layout = QVBoxLayout(self)
          layout.setContentsMargins(5, 5, 5, 5)

          if PYQTGRAPH_AVAILABLE:
                # Chia làm 2 phần: controls và plot area
                graph_splitter = QSplitter(Qt.Horizontal)

                # Phần controls (chọn tín hiệu)
                control_widget = QWidget()
                control_layout = QVBoxLayout(control_widget)
                control_layout.addWidget(QLabel("Chọn Tín hiệu để Vẽ:"))
                self.signalListWidget = QListWidget()
                self.signalListWidget.setSelectionMode(QListWidget.ExtendedSelection) # Chọn nhiều
                self.signalListWidget.itemSelectionChanged.connect(self._plot_selected_signals)
                control_layout.addWidget(self.signalListWidget)
                control_widget.setMinimumWidth(150) # Đảm bảo control ko quá hẹp
                control_widget.setMaximumWidth(300)


                # Phần plot
                self.plotWidget = pg.PlotWidget()
                self.plotWidget.setBackground('w') # Nền trắng
                self.plotWidget.showGrid(x=True, y=True)
                self.plotWidget.addLegend()

                graph_splitter.addWidget(control_widget)
                graph_splitter.addWidget(self.plotWidget)
                graph_splitter.setSizes([200, 600]) # Kích thước ban đầu

                layout.addWidget(graph_splitter)

          else:
                layout.addWidget(QLabel("Thư viện 'pyqtgraph' không có sẵn. Không thể hiển thị đồ thị."))

          self.signal_time_series = {} # Dữ liệu đã xử lý để vẽ

     def update_content(self, network_id, network_data):
          super().update_content(network_id, network_data)
          self.signal_time_series = network_data.get('signal_time_series', {})
          db = network_data.get('db', None)

          if PYQTGRAPH_AVAILABLE:
                self.plotWidget.clear() # Xóa đồ thị cũ
                self.signalListWidget.clear() # Xóa danh sách signal cũ

                if db:
                     # Lấy danh sách tín hiệu từ DBC
                     signal_names = sorted([sig.name for msg in db.messages for sig in msg.signals])
                     # Chỉ thêm các signal có dữ liệu timeseries
                     signals_with_data = [name for name in signal_names if name in self.signal_time_series]
                     self.signalListWidget.addItems(signals_with_data)
                     print(f"Graph tab updated for {network_id}. Signals with data: {len(signals_with_data)}")
                # Nếu có tín hiệu được chọn sẵn thì vẽ lại? Hoặc để user tự chọn lại.


     def _plot_selected_signals(self):
          if not PYQTGRAPH_AVAILABLE: return

          self.plotWidget.clear() # Xóa plot cũ trước khi vẽ lại
          self.plotWidget.addLegend(offset=(-30, 30)) # Thêm lại legend

          selected_items = self.signalListWidget.selectedItems()
          if not selected_items:
               return

          pens = [pg.mkPen(color=c, width=2) for c in ['b', 'r', 'g', 'c', 'm', 'y', 'k'] * 5] # Danh sách màu bút vẽ

          plot_count = 0
          for idx, item in enumerate(selected_items):
                sig_name = item.text()
                if sig_name in self.signal_time_series:
                     timestamps, values = self.signal_time_series[sig_name]
                     if timestamps and values and len(timestamps) == len(values):
                          # Kiểm tra kiểu dữ liệu của timestamp (phải là số)
                          if all(isinstance(t, (int, float)) for t in timestamps):
                                # Kiểm tra kiểu dữ liệu của value (chỉ vẽ nếu là số)
                                numeric_timestamps = []
                                numeric_values = []
                                for t, v in zip(timestamps, values):
                                    if isinstance(v, (int, float)):
                                        numeric_timestamps.append(t)
                                        numeric_values.append(v)

                                if numeric_timestamps and numeric_values:
                                     pen = pens[plot_count % len(pens)]
                                     # Cắt bớt dữ liệu nếu quá lớn để tránh treo (ví dụ: 50k điểm)
                                     max_points = 50000
                                     if len(numeric_timestamps) > max_points:
                                         step = len(numeric_timestamps) // max_points
                                         sampled_ts = numeric_timestamps[::step]
                                         sampled_val = numeric_values[::step]
                                         self.plotWidget.plot(sampled_ts, sampled_val, pen=pen, name=f"{sig_name} (sampled)")
                                     else:
                                         self.plotWidget.plot(numeric_timestamps, numeric_values, pen=pen, name=sig_name)
                                     plot_count += 1
                                else:
                                     print(f"Cảnh báo: Tín hiệu '{sig_name}' không có dữ liệu số để vẽ.")
                          else:
                               print(f"Cảnh báo: Timestamp của tín hiệu '{sig_name}' không phải dạng số.")
                     else:
                          print(f"Cảnh báo: Dữ liệu timeseries của '{sig_name}' không hợp lệ hoặc rỗng.")
                else:
                     print(f"Cảnh báo: Không tìm thấy dữ liệu timeseries cho '{sig_name}'.")


class LoggingTab(BaseNetworkTab):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setAlignment(Qt.AlignTop) # Căn các widget lên trên

        self.statusLabel = QLabel("Trạng thái Log: Đang dừng")
        layout.addWidget(self.statusLabel)

        # Layout chọn file
        file_layout = QHBoxLayout()
        self.logPathEdit = QLineEdit()
        self.logPathEdit.setPlaceholderText("Đường dẫn file log (.csv)")
        self.logPathEdit.setReadOnly(True) # Chỉ hiển thị
        select_button = QPushButton("Chọn File Log...")
        select_button.clicked.connect(self._request_select_log_file)
        file_layout.addWidget(QLabel("File Log:"))
        file_layout.addWidget(self.logPathEdit, 1)
        file_layout.addWidget(select_button)
        layout.addLayout(file_layout)

        # Nút Start/Stop
        self.toggleLogButton = QPushButton(QIcon.fromTheme("media-record"), "Bắt đầu Ghi")
        self.toggleLogButton.setCheckable(True) # Làm nút bật/tắt
        self.toggleLogButton.toggled.connect(self._handle_toggle_log)
        layout.addWidget(self.toggleLogButton)

        # Thêm khoảng trống
        layout.addStretch(1)

    def _request_select_log_file(self):
        if self.current_network_id:
            self.selectLogFileRequested.emit(self.current_network_id)

    def _handle_toggle_log(self, checked):
        if self.current_network_id:
            # Chỉ gửi yêu cầu nếu có đường dẫn file log hợp lệ
            if checked and not self.network_data.get('log_path'):
                 QMessageBox.warning(self, "Thiếu File Log", "Vui lòng chọn file log trước khi bắt đầu ghi.")
                 self.toggleLogButton.setChecked(False) # Bật lại nút về trạng thái tắt
                 return
            self.toggleLoggingRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data):
        super().update_content(network_id, network_data)
        is_logging = network_data.get('is_logging', False)
        log_path = network_data.get('log_path', None)

        self.logPathEdit.setText(log_path if log_path else "")

        # Cập nhật trạng thái nút và label (tránh trigger tín hiệu toggled)
        was_blocked = self.toggleLogButton.blockSignals(True)
        self.toggleLogButton.setChecked(is_logging)
        if is_logging:
            self.statusLabel.setText(f"Trạng thái Log: Đang ghi vào {os.path.basename(log_path)}")
            self.toggleLogButton.setText("Dừng Ghi")
            self.toggleLogButton.setIcon(QIcon.fromTheme("media-playback-stop"))
        else:
            self.statusLabel.setText("Trạng thái Log: Đang dừng")
            self.toggleLogButton.setText("Bắt đầu Ghi")
            self.toggleLogButton.setIcon(QIcon.fromTheme("media-record"))
        self.toggleLogButton.blockSignals(was_blocked)

# --- Cửa sổ chính ---
class MultiCanManagerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.networks_data = {} # { network_id: { 'name': ..., 'dbc_path':..., 'db':..., ... }, ... }
        self.workers = {} # { worker_id (e.g., f"{net_id}_dbc"): worker_thread }
        self.next_network_id_counter = 1
        self.current_selected_network_id = None

        self.initUI()

    def initUI(self):
        self.setWindowTitle("Multi-Network CAN Manager")
        self.setGeometry(100, 100, 1400, 900)

        # --- Menu ---
        self.setup_menu()

        # --- Bố cục chính với Splitter ---
        self.splitter = QSplitter(Qt.Horizontal)

        # --- Panel Trái: Cây Mạng ---
        self.networkTreeWidget = QTreeWidget()
        self.setup_network_tree()
        # Đặt độ rộng ban đầu cố định cho panel trái (có thể chỉnh sau bằng splitter)
        # self.networkTreeWidget.setMinimumWidth(200)
        # self.networkTreeWidget.setMaximumWidth(400)
        tree_container = QWidget() # Bọc tree vào widget để set max width
        tree_layout = QVBoxLayout(tree_container)
        tree_layout.setContentsMargins(0,0,0,0)
        tree_layout.addWidget(self.networkTreeWidget)
        tree_container.setMaximumWidth(350)
        self.splitter.addWidget(tree_container)


        # --- Panel Phải: Các Tab Chi tiết ---
        self.detailsTabWidget = QTabWidget()
        self.setup_detail_tabs()
        self.splitter.addWidget(self.detailsTabWidget)

        # Cấu hình Splitter
        self.splitter.setSizes([300, 1100]) # Kích thước ban đầu
        self.splitter.setChildrenCollapsible(False)

        self.setCentralWidget(self.splitter)

        # --- Thanh trạng thái ---
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusLabel = QLabel("Sẵn sàng")
        self.statusBar.addWidget(self.statusLabel, 1)

        self.show()

    def setup_menu(self):
        menu_bar = self.menuBar()
        # File Menu
        file_menu = menu_bar.addMenu("&File")
        exit_action = QAction(QIcon.fromTheme("application-exit"), "&Thoát", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Network Menu
        network_menu = menu_bar.addMenu("&Network")
        add_net_action = QAction(QIcon.fromTheme("network-wired"), "&Thêm Mạng CAN Mới", self)
        add_net_action.triggered.connect(self.add_new_network)
        network_menu.addAction(add_net_action)
        # Có thể thêm "Xóa Mạng CAN" ở đây, cần xử lý network đang chọn

        # Help Menu
        help_menu = menu_bar.addMenu("&Help")
        about_action = QAction("&Giới thiệu", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)

    def setup_network_tree(self):
        self.networkTreeWidget.setHeaderHidden(True) # Chỉ hiển thị tên mạng
        root_item = QTreeWidgetItem(self.networkTreeWidget, ["CAN Networks"])
        self.networkTreeWidget.addTopLevelItem(root_item)
        root_item.setExpanded(True)
        root_item.setFlags(root_item.flags() & ~Qt.ItemIsEditable) # Root không cho sửa tên

        self.networkTreeWidget.itemSelectionChanged.connect(self.on_network_selection_changed)
        self.networkTreeWidget.itemChanged.connect(self.on_network_item_changed) # Để xử lý đổi tên

    def setup_detail_tabs(self):
        self.dbcTab = DbcConfigTab()
        self.traceTab = TraceMessagesTab()
        self.signalsTab = SignalDataTab()
        self.graphTab = GraphingTab()
        self.logTab = LoggingTab()

        self.detailsTabWidget.addTab(self.dbcTab, QIcon.fromTheme("database"), "Cấu hình DBC")
        self.detailsTabWidget.addTab(self.traceTab, QIcon.fromTheme("text-x-generic"), "Trace/Messages")
        self.detailsTabWidget.addTab(self.signalsTab, QIcon.fromTheme("view-list-details"), "Dữ liệu Tín hiệu")
        self.detailsTabWidget.addTab(self.graphTab, QIcon.fromTheme("utilities-system-monitor"), "Đồ thị")
        self.detailsTabWidget.addTab(self.logTab, QIcon.fromTheme("document-save"), "Thu Log")

        # Kết nối tín hiệu từ các tab đến MainWindow để xử lý
        self.dbcTab.loadDbcRequested.connect(self.handle_load_dbc)
        self.traceTab.loadTraceRequested.connect(self.handle_load_trace)
        self.logTab.selectLogFileRequested.connect(self.handle_select_log_file)
        self.logTab.toggleLoggingRequested.connect(self.handle_toggle_logging)

        # Kết nối tín hiệu cập nhật signal từ TraceTab -> SignalTab
        self.traceTab.signalValueUpdate.connect(self.handle_signal_value_update)

        # Vô hiệu hóa các tab ban đầu vì chưa chọn network nào
        self.detailsTabWidget.setEnabled(False)

    # --- Quản lý Mạng ---
    def add_new_network(self):
        network_id = str(uuid.uuid4()) # Tạo ID duy nhất
        network_name = f"CAN Network {self.next_network_id_counter}"
        self.next_network_id_counter += 1

        # Tạo dữ liệu ban đầu
        self.networks_data[network_id] = {
            "name": network_name,
            "dbc_path": None,
            "db": None,
            "trace_path": None,
            "trace_data": [],
            "signal_time_series": {}, # Dữ liệu cho đồ thị
            "latest_signal_values": {}, # Giá trị cuối cùng cho bảng signals
            "log_path": None,
            "is_logging": False,
            "logging_worker": None # Tham chiếu đến worker nếu đang log
        }

        # Thêm vào cây
        root = self.networkTreeWidget.topLevelItem(0)
        network_item = QTreeWidgetItem(root, [network_name])
        network_item.setData(0, Qt.UserRole, network_id) # Lưu ID vào item data
        network_item.setFlags(network_item.flags() | Qt.ItemIsEditable) # Cho phép sửa tên
        root.addChild(network_item)

        # Tự động chọn network mới tạo
        self.networkTreeWidget.setCurrentItem(network_item)
        self.statusLabel.setText(f"Đã thêm mạng mới: {network_name}")

    def on_network_selection_changed(self):
        selected_items = self.networkTreeWidget.selectedItems()
        if not selected_items:
            self.current_selected_network_id = None
            self.detailsTabWidget.setEnabled(False) # Vô hiệu hóa tabs nếu không có gì được chọn
            self.statusLabel.setText("Không có mạng nào được chọn")
            return

        selected_item = selected_items[0]
        network_id = selected_item.data(0, Qt.UserRole)

        # Chỉ xử lý nếu item được chọn là một network (có ID)
        if network_id and network_id in self.networks_data:
            self.current_selected_network_id = network_id
            network_data = self.networks_data[network_id]
            network_name = network_data.get("name", "N/A")

            # Kích hoạt và cập nhật nội dung các tab
            self.detailsTabWidget.setEnabled(True)
            print(f"Selection changed to Network ID: {network_id}, Name: {network_name}")
            # Gọi update_content cho TẤT CẢ các tab
            for i in range(self.detailsTabWidget.count()):
                 tab_widget = self.detailsTabWidget.widget(i)
                 if isinstance(tab_widget, BaseNetworkTab):
                      tab_widget.update_content(network_id, network_data)

            self.statusLabel.setText(f"Đang xem mạng: {network_name}")
        else:
            # Nếu chọn root item hoặc item không hợp lệ
            self.current_selected_network_id = None
            self.detailsTabWidget.setEnabled(False)
            self.statusLabel.setText("Vui lòng chọn một mạng CAN")

    def on_network_item_changed(self, item, column):
        """Xử lý khi tên mạng trong cây bị đổi."""
        if column == 0: # Chỉ xử lý cột tên
            network_id = item.data(0, Qt.UserRole)
            if network_id and network_id in self.networks_data:
                new_name = item.text(0)
                old_name = self.networks_data[network_id]['name']
                if new_name != old_name:
                    self.networks_data[network_id]['name'] = new_name
                    self.statusLabel.setText(f"Đã đổi tên '{old_name}' thành '{new_name}'")
                    # Nếu đây là network đang được chọn, cập nhật status bar chính xác hơn
                    if network_id == self.current_selected_network_id:
                        self.statusLabel.setText(f"Đang xem mạng: {new_name}")


    # --- Xử lý yêu cầu từ các Tab ---

    def handle_load_dbc(self, network_id):
        if not network_id or network_id not in self.networks_data: return
        worker_id = f"{network_id}_dbc"
        if worker_id in self.workers and self.workers[worker_id].isRunning():
             QMessageBox.information(self, "Đang xử lý", f"Đang tải file DBC cho mạng {self.networks_data[network_id]['name']}...")
             return

        current_path = self.networks_data[network_id].get('dbc_path', None)
        dir_path = os.path.dirname(current_path) if current_path else ""

        file_path, _ = QFileDialog.getOpenFileName(self, f"Chọn File DBC cho Mạng {self.networks_data[network_id]['name']}", dir_path, "CAN Database (*.dbc)")
        if file_path:
            self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: Bắt đầu tải DBC...")
            worker = DbcLoadingWorker(network_id, file_path)
            worker.finished.connect(self.on_dbc_loaded)
            worker.progress.connect(self.update_network_status)
            self.workers[worker_id] = worker
            worker.start()

    def handle_load_trace(self, network_id):
        if not network_id or network_id not in self.networks_data: return
        worker_id = f"{network_id}_trace"
        if worker_id in self.workers and self.workers[worker_id].isRunning():
            QMessageBox.information(self, "Đang xử lý", f"Đang tải file Trace cho mạng {self.networks_data[network_id]['name']}...")
            return

        current_path = self.networks_data[network_id].get('trace_path', None)
        dir_path = os.path.dirname(current_path) if current_path else ""

        file_path, _ = QFileDialog.getOpenFileName(self, f"Chọn File Trace CSV cho Mạng {self.networks_data[network_id]['name']}", dir_path, "CSV Files (*.csv);;All Files (*)")
        if file_path:
            self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: Bắt đầu tải Trace...")
            # Lấy db hiện tại để worker có thể giải mã luôn
            db_obj = self.networks_data[network_id].get('db', None)
            worker = TraceLoadingWorker(network_id, file_path, db_obj)
            worker.finished.connect(self.on_trace_loaded)
            worker.progress.connect(self.update_network_status)
            worker.progress_percent.connect(self.update_network_progress_percent)
            self.workers[worker_id] = worker
            worker.start()

    def handle_select_log_file(self, network_id):
        if not network_id or network_id not in self.networks_data: return
        current_path = self.networks_data[network_id].get('log_path', None)
        dir_path = os.path.dirname(current_path) if current_path else ""

        file_path, _ = QFileDialog.getSaveFileName(self, f"Chọn File Log CSV cho Mạng {self.networks_data[network_id]['name']}", dir_path, "CSV Files (*.csv)")
        if file_path:
             # Đảm bảo đuôi file là .csv
             if not file_path.lower().endswith(".csv"):
                 file_path += ".csv"

             self.networks_data[network_id]['log_path'] = file_path
             self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: Đã chọn file log.")
             # Cập nhật lại tab Log nếu network này đang được chọn
             if network_id == self.current_selected_network_id:
                 self.logTab.update_content(network_id, self.networks_data[network_id])

    def handle_toggle_logging(self, network_id):
        if not network_id or network_id not in self.networks_data: return
        network_info = self.networks_data[network_id]
        is_currently_logging = network_info.get('is_logging', False)
        log_path = network_info.get('log_path', None)
        worker_id = f"{network_id}_logging"

        if not is_currently_logging: # --- Bắt đầu ghi ---
            if not log_path:
                 QMessageBox.warning(self, "Thiếu File Log", f"Vui lòng chọn file log cho mạng {network_info['name']} trước.")
                 # Cần cập nhật lại trạng thái nút trên tab log
                 if network_id == self.current_selected_network_id: self.logTab.update_content(network_id, network_info)
                 return

            if worker_id in self.workers and self.workers[worker_id].isRunning():
                 print(f"Cảnh báo: Worker log cho {network_id} dường như đang chạy?")
                 # Có thể dừng worker cũ ở đây nếu logic cho phép
                 self.workers[worker_id].stop()
                 self.workers[worker_id].wait(500) # Chờ worker dừng

            # Tạo worker mới và bắt đầu
            log_worker = LoggingWorker(network_id, log_path)
            log_worker.status.connect(self.update_network_status)
            log_worker.error.connect(self.show_network_error)
            # Kết nối nguồn dữ liệu (trace_data) vào queue của worker (đơn giản hóa)
            # Trong ứng dụng thực tế, cần có cơ chế push dữ liệu (từ live hoặc khi đọc trace)
            # Ở đây, tạm thời add dữ liệu trace đã có khi bắt đầu log (không real-time)
            if network_info.get('trace_data'):
                for msg_row in network_info['trace_data']:
                    log_worker.add_message_to_queue(msg_row)

            network_info['is_logging'] = True
            network_info['logging_worker'] = log_worker
            self.workers[worker_id] = log_worker
            log_worker.start()
            self.statusLabel.setText(f"Net {network_info['name']}: Đang ghi log...")

        else: # --- Dừng ghi ---
            log_worker = network_info.get('logging_worker')
            if log_worker and log_worker.isRunning():
                log_worker.stop() # Yêu cầu worker dừng
                # Worker sẽ tự emit status khi dừng hẳn
                self.statusLabel.setText(f"Net {network_info['name']}: Đang dừng ghi log...")
                # Không xóa worker ngay, chờ nó kết thúc trong run()
            else:
                 print(f"Cảnh báo: Không tìm thấy worker log đang chạy cho {network_id} để dừng.")
                 network_info['is_logging'] = False # Cập nhật lại trạng thái

            network_info['is_logging'] = False
            network_info['logging_worker'] = None
            if worker_id in self.workers: # Có thể dọn dẹp worker cũ ở đây
                 pass # del self.workers[worker_id] # Hoặc để worker tự kết thúc

        # Cập nhật lại tab Log
        if network_id == self.current_selected_network_id:
            self.logTab.update_content(network_id, network_info)


    # --- Slots xử lý kết quả từ Workers ---

    def on_dbc_loaded(self, network_id, db_or_none, path_or_error):
        worker_id = f"{network_id}_dbc"
        if worker_id in self.workers: del self.workers[worker_id] # Dọn dẹp worker

        if network_id not in self.networks_data: return # Network đã bị xóa?

        network_info = self.networks_data[network_id]
        if db_or_none is not None:
            network_info['db'] = db_or_none
            network_info['dbc_path'] = path_or_error
            self.statusLabel.setText(f"Net {network_info['name']}: Đã tải DBC thành công.")
            # Xóa dữ liệu signal cũ vì DBC thay đổi
            network_info['latest_signal_values'] = {}
            network_info['signal_time_series'] = {}
             # Nếu đang xem network này, cập nhật tab DBC, Signal, Graph
            if network_id == self.current_selected_network_id:
                self.dbcTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info)
                self.graphTab.update_content(network_id, network_info)
                 # Nếu có trace data, cần re-decode hoặc nhắc người dùng tải lại trace
                if network_info.get('trace_data'):
                     # Tùy chọn: Tự động chạy lại worker trace để giải mã với DBC mới
                     # Hoặc đơn giản là cập nhật bảng trace mà không có dữ liệu giải mã
                      self.traceTab.populate_trace_table(network_info['trace_data'], db_or_none)
                      QMessageBox.information(self,"DBC đã cập nhật", "DBC đã được tải lại. Dữ liệu trace đã được hiển thị lại (cần giải mã lại nếu muốn cập nhật đầy đủ).")

        else:
            network_info['db'] = None
            network_info['dbc_path'] = None
            self.show_network_error(network_id, f"Lỗi tải DBC:\n{path_or_error}")
            # Cập nhật tab nếu đang hiển thị
            if network_id == self.current_selected_network_id:
                self.dbcTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info) # Xóa signal list
                self.graphTab.update_content(network_id, network_info) # Xóa graph


    def on_trace_loaded(self, network_id, trace_data_or_none, signal_timeseries, path_or_error):
        worker_id = f"{network_id}_trace"
        if worker_id in self.workers: del self.workers[worker_id]

        if network_id not in self.networks_data: return

        network_info = self.networks_data[network_id]
        if trace_data_or_none is not None:
            network_info['trace_data'] = trace_data_or_none
            network_info['trace_path'] = path_or_error
            network_info['signal_time_series'] = signal_timeseries # Lưu dữ liệu đã xử lý
             # Tính toán lại latest_signal_values từ timeseries (hoặc làm trong worker)
            network_info['latest_signal_values'] = self._get_latest_values_from_timeseries(signal_timeseries)

            self.statusLabel.setText(f"Net {network_info['name']}: Đã tải Trace thành công ({len(trace_data_or_none)} msgs).")

            # Cập nhật các tab liên quan nếu network đang được chọn
            if network_id == self.current_selected_network_id:
                self.traceTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info)
                self.graphTab.update_content(network_id, network_info)

             # Nếu đang ghi log, cần có cơ chế đưa trace_data này vào logging_worker

            if network_info.get('is_logging') and network_info.get('logging_worker'):
                  lw = network_info['logging_worker']
                  for msg_row in trace_data_or_none:
                       lw.add_message_to_queue(msg_row)
                  self.update_network_status(network_id, f"Đã thêm {len(trace_data_or_none)} tin nhắn từ trace vào hàng đợi log.")

        else:
            network_info['trace_data'] = []
            network_info['trace_path'] = None
            network_info['signal_time_series'] = {}
            network_info['latest_signal_values'] = {}
            self.show_network_error(network_id, f"Lỗi tải Trace:\n{path_or_error}")
            if network_id == self.current_selected_network_id:
                self.traceTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info)
                self.graphTab.update_content(network_id, network_info)

    def _get_latest_values_from_timeseries(self, signal_timeseries):
        """Lấy giá trị cuối cùng từ dữ liệu timeseries đã xử lý."""
        latest_values = {}
        for sig_name, (timestamps, values) in signal_timeseries.items():
             if timestamps and values:
                  # Giả sử timestamps đã được sắp xếp tăng dần
                  latest_values[sig_name] = (values[-1], timestamps[-1])
        return latest_values


    # --- Xử lý cập nhật Signal ---
    def handle_signal_value_update(self, network_id, signal_name, value, timestamp_obj):
         """Xử lý khi có giá trị signal mới được giải mã (từ TraceTab)."""
         if network_id in self.networks_data:
              # Cập nhật giá trị mới nhất trong dữ liệu chính
              self.networks_data[network_id]['latest_signal_values'][signal_name] = (value, timestamp_obj)

              # Nếu network này đang hiển thị và tab Signal đang hoạt động
              if network_id == self.current_selected_network_id:
                   self.signalsTab.update_signal_value(signal_name, value, timestamp_obj)
                   # Tùy chọn: Cập nhật đồ thị live (nếu đang vẽ signal này)
                   # self.graphTab.append_data_point(signal_name, timestamp_obj, value)
         # else: print(f"Received signal update for unknown network: {network_id}")


    # --- Cập nhật trạng thái và Lỗi ---

    def update_network_status(self, network_id, message):
        if network_id in self.networks_data:
             net_name = self.networks_data[network_id]['name']
             self.statusLabel.setText(f"Net {net_name}: {message}")
             # Có thể cập nhật label trạng thái riêng của từng tab nếu cần
        else:
             self.statusLabel.setText(message) # Hiển thị chung nếu ko rõ network

    def update_network_progress_percent(self, network_id, percent):
         if network_id in self.networks_data:
             net_name = self.networks_data[network_id]['name']
             self.statusLabel.setText(f"Net {net_name}: Đang xử lý... {percent}%")


    def show_network_error(self, network_id, error_message):
        """Hiển thị lỗi liên quan đến một mạng cụ thể."""
        net_name = self.networks_data.get(network_id, {}).get('name', f"ID {network_id}")
        self.statusLabel.setText(f"Net {net_name}: Có lỗi xảy ra.")
        QMessageBox.critical(self, f"Lỗi - Mạng {net_name}", error_message)

    # --- Tiện ích khác ---
    def show_about_dialog(self):
        QMessageBox.about(self, "Giới thiệu",
                          "<b>Multi-Network CAN Manager</b><br>"
                          "Ứng dụng quản lý và giám sát nhiều mạng CAN.<br>"
                          "Sử dụng PyQt5, cantools, pyqtgraph.<br><br>"
                          "(c) 2023-2024")

    def closeEvent(self, event):
        # Kiểm tra các worker đang chạy
        running_workers = {wid: w for wid, w in self.workers.items() if w.isRunning()}
        if running_workers:
             worker_descs = []
             for wid, w in running_workers.items():
                 # Trích xuất network_id từ worker_id
                 net_id_part = wid.split('_')[0]
                 net_name = self.networks_data.get(net_id_part, {}).get('name', net_id_part)
                 task_type = wid.split('_')[-1] # dbc, trace, logging
                 worker_descs.append(f"{net_name} ({task_type})")

             reply = QMessageBox.question(self, 'Xác nhận Thoát',
                                          f"Các tác vụ nền sau đang chạy:\n- " + "\n- ".join(worker_descs) + \
                                          "\n\nThoát ứng dụng sẽ cố gắng dừng chúng. Tiếp tục?",
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
             if reply == QMessageBox.Yes:
                  print("Đang yêu cầu các worker dừng...")
                  for worker_id, worker in running_workers.items():
                      try:
                          if hasattr(worker, 'stop'): # Ưu tiên gọi hàm stop() nếu có (như LoggingWorker)
                              worker.stop()
                          elif worker.isRunning():
                              # worker.terminate() # Không nên dùng terminate
                              worker.quit() # Yêu cầu thoát vòng lặp sự kiện
                      except Exception as e:
                           print(f"Lỗi khi yêu cầu dừng worker {worker_id}: {e}")
                      # Chờ một chút để luồng có cơ hội xử lý
                      if not worker.wait(300): # Chờ tối đa 300ms
                           print(f"Cảnh báo: Worker {worker_id} không dừng kịp thời.")
                  event.accept()
             else:
                  event.ignore()
        else:
             event.accept()

# --- Chạy ứng dụng ---
if __name__ == '__main__':
    app = QApplication(sys.argv)
    # Tùy chọn: Đặt style
    # app.setStyle('Fusion')
    manager = MultiCanManagerApp()
    sys.exit(app.exec_())
