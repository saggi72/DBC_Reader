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
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,QGridLayout,
        QAction, QFileDialog, QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
        QStatusBar, QMessageBox, QSplitter, QHeaderView, QLabel, QMenuBar,QMenu,
        QTabWidget, QPushButton, QLineEdit, QStackedWidget, QComboBox, QGroupBox,
        QScrollArea, QTextEdit, QListWidget, QToolBar # Thêm các widget cần thiết
    )
    from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject, QSize, QTimer, QMutex # Thêm QMutex nếu cần thread-safety kỹ hơn
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

# Thư viện cho Chẩn đoán
try:
    import odxtools
except ImportError:
    print("Lỗi: Thư viện 'odxtools' chưa được cài đặt.")
    print("Vui lòng cài đặt bằng lệnh: pip install odxtools")
    sys.exit(1)

try:
    import can
except ImportError:
    print("Lỗi: Thư viện 'python-can' chưa được cài đặt.")
    print("Vui lòng cài đặt bằng lệnh: pip install python-can")
    # print("Bạn cũng cần cài đặt backend phù hợp, ví dụ: pip install python-can[vector]")
    sys.exit(1)

try:
    import isotp
except ImportError:
     print("Lỗi: Thư viện 'isotp' chưa được cài đặt (cho ISO 15765-2).")
     print("Vui lòng cài đặt bằng lệnh: pip install isotp")
     #sys.exit(1)


# --- HELPER FUNCTIONS ---
def get_can_bus_for_network(network_id, main_app_instance):
    """Placeholder: Retrieves the python-can bus object for the network.
       THIS NEEDS TO BE IMPLEMENTED PROPERLY in the main app based on
       how connections are managed.
    """
    if network_id in main_app_instance.networks_data:
        # Assuming the bus object is stored like this after connection
        return main_app_instance.networks_data[network_id].get('can_bus', None)
    return None

# --- WORKER THREADS ---

class DbcLoadingWorker(QThread):
    finished = pyqtSignal(str, object, str) # network_id, db_or_none, path_or_error
    progress = pyqtSignal(str, str)        # network_id, message

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
        # self.queue_mutex = QMutex() # Optional: for thread-safe queue access

    def run(self):
        self._is_running = True
        try:
            self.status.emit(self.network_id, f"Bắt đầu ghi log vào: {os.path.basename(self.log_file_path)}")
            # Mở file và tạo CSV writer
            self.file = open(self.log_file_path, 'w', newline='', encoding='utf-8')
            self.writer = csv.writer(self.file)
            self.writer.writerow(['Timestamp', 'ID_Hex', 'DLC', 'Data_Hex']) # Viết header

            while self._is_running:
                # Xử lý message trong queue
                messages_to_write = []
                # self.queue_mutex.lock() # Lock if using mutex
                while self.message_queue:
                    messages_to_write.append(self.message_queue.pop(0))
                # self.queue_mutex.unlock() # Unlock if using mutex

                if messages_to_write:
                    self.writer.writerows(messages_to_write) # Ghi nhiều dòng một lúc
                    self.file.flush() # Ghi xuống disk thường xuyên hơn?

                self.msleep(50) # Ngủ một chút để tránh CPU load cao

            # Ghi nốt những gì còn sót lại trong queue khi dừng
            # self.queue_mutex.lock()
            messages_to_write = self.message_queue[:]
            self.message_queue.clear()
            # self.queue_mutex.unlock()
            if messages_to_write:
                 self.writer.writerows(messages_to_write)
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
         # Cần cơ chế thread-safe nếu nhiều luồng cùng add (dùng QMutex hoặc collections.deque)
         if self._is_running and self.writer:
              # self.queue_mutex.lock()
              self.message_queue.append(message_list)
              # self.queue_mutex.unlock()

# --- DIAGNOSTIC WORKERS ---

class DiagFileLoadingWorker(QThread):
    """Worker to load and parse PDX/ODX files."""
    finished = pyqtSignal(str, object, str) # network_id, odx_database_or_none, error_string_or_path

    def __init__(self, network_id, file_path):
        super().__init__()
        self.network_id = network_id
        self.file_path = file_path

    def run(self):
        try:
            print(f"Attempting to load diagnostic file: {self.file_path}")
            db = None
            if self.file_path.lower().endswith(".pdx"):
                db = odxtools.load_pdx_file(self.file_path)
            elif self.file_path.lower().endswith((".odx", ".odx-d")): # Basic ODX file types
                 db = odxtools.load_odx_file(self.file_path)
            # Thêm hỗ trợ CDD nếu có thư viện (hiện tại không có thư viện chuẩn)
            # elif self.file_path.lower().endswith(".cdd"):
            #     raise NotImplementedError("Phân tích cú pháp CDD chưa được hỗ trợ.")
            else:
                raise ValueError("Loại file không được hỗ trợ. Vui lòng chọn file .pdx hoặc .odx/.odx-d.")

            print(f"Successfully loaded diagnostic file for network {self.network_id}")
            self.finished.emit(self.network_id, db, self.file_path)

        except FileNotFoundError:
            self.finished.emit(self.network_id, None, f"Lỗi: Không tìm thấy file '{self.file_path}'")
        except Exception as e:
            error_details = traceback.format_exc()
            self.finished.emit(self.network_id, None, f"Lỗi đọc file chẩn đoán:\n{e}\n\nChi tiết:\n{error_details}")

class DiagnosticWorker(QThread):
    """Worker to perform a diagnostic request."""
    # Signals: network_id, success_bool, result_data_dict_or_error_str, raw_request_bytes, raw_response_bytes
    finished = pyqtSignal(str, bool, object, bytes, bytes)
    progress = pyqtSignal(str, str) # network_id, message

    def __init__(self, network_id, main_app_ref, request_details):
        """
        Args:
            network_id: ID of the network.
            main_app_ref: Reference to the main application window to access data.
            request_details: Dict defining the request, e.g.,
                {'type': 'read_did', 'did': 0xF190}
                {'type': 'read_dtc', 'subfunction': 0x02}
                {'type': 'clear_dtc', 'group': 0xFFFFFF}
                {'type': 'ecu_reset', 'subfunction': 0x01}
                # ... etc.
        """
        super().__init__()
        self.network_id = network_id
        self.main_app = main_app_ref # Be careful with direct references if main_app can be closed
        self.request_details = request_details
        self._is_running = True

    def run(self):
        raw_request = b''
        raw_response = b''
        ecu: odxtools.DiagLayer = None # Type hint
        isotp_stack = None
        can_bus = None

        try:
            if not self.network_id or self.network_id not in self.main_app.networks_data:
                raise ValueError("Network ID không hợp lệ hoặc không tìm thấy.")

            network_data = self.main_app.networks_data[self.network_id]
            ecu = network_data.get('selected_ecu_diag_layer')
            odx_db = network_data.get('odx_database')

            if not ecu or not isinstance(ecu, odxtools.DiagLayer):
                 raise ValueError("Chưa chọn ECU hợp lệ từ file ODX/PDX.")

            # --- 1. Get CAN Bus and Check Connection ---
            can_bus = get_can_bus_for_network(self.network_id, self.main_app)
            if can_bus is None:
                 raise ConnectionError("Mạng CAN chưa được kết nối (bus không tồn tại).")

            # --- 2. Get Addressing Info from ODX ---
            # !!! PLACEHOLDER - Needs real ODX parsing logic !!!
            tx_id = None
            rx_id = None
            try:
                 # Versuche, IDs aus den ODX Kommunikationsparametern zu extrahieren
                 # This is complex and requires navigating odxtools based on your ODX file structure
                 # Example using get_can_receive_id/get_can_send_id (may not work for all ODX)
                 tx_id = ecu.get_can_receive_id() # ECU receives requests on this ID
                 rx_id = ecu.get_can_send_id()    # ECU sends responses from this ID

                 if tx_id is None or rx_id is None:
                      self.progress.emit(self.network_id, "Cảnh báo: Không tìm thấy ID req/resp trực tiếp, thử tìm trong CommParams...")
                      # Add more sophisticated logic here to parse ecu.communication_parameters
                      # This might involve finding the correct PhysConn, Protocol, etc.

                      # --- Fallback / Hardcode (Remove in production) ---
                      if tx_id is None: tx_id = 0x7E0 # Example PhysReq ID
                      if rx_id is None: rx_id = 0x7E8 # Example PhysResp ID
                      self.progress.emit(self.network_id, f"Cảnh báo: Sử dụng ID chẩn đoán mặc định/dự phòng ({tx_id:X}/{rx_id:X}). Nên định nghĩa rõ trong ODX.")
                 else:
                     self.progress.emit(self.network_id, f"Sử dụng ID từ ODX: Tx={tx_id:X}, Rx={rx_id:X}")

            except Exception as e:
                 self.progress.emit(self.network_id, f"Lỗi khi lấy ID từ ODX: {e}. Sử dụng dự phòng.")
                 tx_id = 0x7E0
                 rx_id = 0x7E8

            if tx_id is None or rx_id is None:
                 raise ValueError("Không thể xác định ID Request/Response chẩn đoán.")


            # --- 3. Encode Request using odxtools ---
            service_name = None
            params = {} # Use dict for named parameters
            req_type = self.request_details['type']
            self.progress.emit(self.network_id, f"Chuẩn bị yêu cầu: {req_type}...")

            # Find the service object
            # ODX lookup can be by short_name or OID. Using get() is safer.
            service = ecu.services.get(req_type) # Try matching type to service short name directly
            if not service:
                # Add lookups for common UDS service names if type doesn't match short_name
                service_mapping = {
                    'read_did': 'ReadDataByIdentifier',
                    'read_dtc': 'ReadDTCInformation',
                    'clear_dtc': 'ClearDiagnosticInformation',
                    'ecu_reset': 'ECUReset',
                    'security_access': 'SecurityAccess',
                    'write_did': 'WriteDataByIdentifier',
                    # Add other mappings as needed
                }
                service_name_lookup = service_mapping.get(req_type)
                if service_name_lookup:
                    service = ecu.services.get(service_name_lookup)

            if not service:
                 raise ValueError(f"Dịch vụ tương ứng với loại '{req_type}' không được định nghĩa trong ODX cho ECU này.")

            service_name = service.short_name # Get the actual short name from the found service

            # --- Populate parameters based on request_details ---
            # !!! Parameter names MUST match those defined in the ODX for the service request !!!
            if req_type == 'read_did':
                did_value = self.request_details['did']
                # Assume the first parameter is the identifier (Risky! Check ODX)
                if not service.request or not service.request.parameters:
                    raise ValueError(f"Service {service_name} không có tham số request được định nghĩa trong ODX.")
                did_param_name = service.request.parameters[0].short_name
                params = {did_param_name: did_value}

            elif req_type == 'read_dtc':
                subfunc = self.request_details['subfunction']
                if not service.request or not service.request.parameters:
                    raise ValueError(f"Service {service_name} không có tham số request được định nghĩa trong ODX.")
                subfunc_param_name = service.request.parameters[0].short_name
                params = {subfunc_param_name: subfunc}
                # Handle optional DTCStatusMask (assuming it's the second param if present)
                if len(service.request.parameters) > 1 and 'mask' in self.request_details:
                     mask_param_name = service.request.parameters[1].short_name
                     params[mask_param_name] = self.request_details['mask']

            elif req_type == 'clear_dtc':
                 group_of_dtc = self.request_details['group']
                 if not service.request or not service.request.parameters:
                     raise ValueError(f"Service {service_name} không có tham số request được định nghĩa trong ODX.")
                 group_param_name = service.request.parameters[0].short_name
                 params = {group_param_name: group_of_dtc}

            elif req_type == 'ecu_reset':
                 reset_type = self.request_details['subfunction']
                 if not service.request or not service.request.parameters:
                     raise ValueError(f"Service {service_name} không có tham số request được định nghĩa trong ODX.")
                 reset_param_name = service.request.parameters[0].short_name
                 params = {reset_param_name: reset_type}

            # --- Add Security Access, Write DID, etc. here ---
            # These require more complex parameter handling and possibly multi-step logic

            else:
                 raise NotImplementedError(f"Mã hóa cho loại yêu cầu '{req_type}' chưa được triển khai.")

            # --- Encode the request ---
            raw_request = service.encode_request(**params)

            if not raw_request:
                 raise ValueError(f"Không thể mã hóa yêu cầu {service_name} (kiểm tra định nghĩa ODX và tham số: {params}).")

            self.progress.emit(self.network_id, f"Đã mã hóa yêu cầu {service_name}: {raw_request.hex()}")

            # --- 4. Setup ISO-TP ---
            addr = isotp.Address(isotp.AddressingMode.Normal_11bit, txid=tx_id, rxid=rx_id)
            # Timeout needs tuning (STmin, Blocksize can also be set if needed)
            isotp_stack = isotp.CanStack(bus=can_bus, address=addr, error_handler=self.isotp_error_handler)
            isotp_stack.set_fc_opts(stmin=5, bs=10) # Example Flow Control options

            # --- 5. Send Request and Receive Response ---
            self.progress.emit(self.network_id, f"Gửi yêu cầu đến ID 0x{tx_id:X}...")
            isotp_stack.send(raw_request)

            self.progress.emit(self.network_id, f"Đang chờ phản hồi từ ID 0x{rx_id:X}...")
            timeout = 5.0 # Adjust timeout as needed (seconds)
            raw_response = isotp_stack.recv(timeout=timeout)

            if raw_response is None:
                raise TimeoutError(f"Không nhận được phản hồi chẩn đoán trong {timeout} giây.")

            self.progress.emit(self.network_id, f"Đã nhận phản hồi: {raw_response.hex()}")

            # --- 6. Decode Response using odxtools ---
            decoded_response = ecu.decode(raw_response)

            self.progress.emit(self.network_id, "Giải mã phản hồi...")

            if decoded_response is None:
                 # Handle cases where decode fails but it might be a valid (unknown) response
                 if raw_response:
                     resp_sid = raw_response[0]
                     if resp_sid == 0x7F: # Negative response SID
                          nrc = raw_response[2] if len(raw_response) > 2 else 0xFF
                          nrc_obj = ecu.negative_responses.get(nrc)
                          nrc_desc = nrc_obj.short_name if nrc_obj else f"Unknown NRC (0x{nrc:02X})"
                          result_data = {
                              'service_name': f"Unknown Service (Req SID: 0x{raw_request[0]:02X})" if raw_request else "Unknown",
                              'response_type': 'Negative',
                              'parameters': {},
                              'nrc': {'code': nrc, 'description': nrc_desc}
                          }
                          error_msg = f"NRC 0x{nrc:02X}: {nrc_desc}"
                          self.progress.emit(self.network_id, error_msg)
                          self.finished.emit(self.network_id, False, result_data, raw_request, raw_response)
                          return # Finished handling this specific case
                     else: # Positive response SID but decode failed
                          raise ValueError(f"Không thể giải mã phản hồi dương hợp lệ (SID: 0x{resp_sid:02X}). Kiểm tra định nghĩa ODX.")
                 else: # No raw response
                    raise ValueError("Không thể giải mã phản hồi rỗng.")


            # Process successfully decoded response
            result_data = {
                'service_name': decoded_response.service.short_name if decoded_response.service else "Unknown",
                'response_type': 'Positive' if decoded_response.positive else 'Negative',
                'parameters': {},
                'nrc': None
            }

            if decoded_response.positive:
                for param_name, param_value in decoded_response.parameters.items():
                    result_data['parameters'][param_name] = param_value # odxtools provides computed value
                self.finished.emit(self.network_id, True, result_data, raw_request, raw_response)
            else: # Negative Response
                nrc_val = decoded_response.parameters.get('ResponseCode', raw_response[2] if len(raw_response) > 2 else 0xFF)
                nrc_obj = ecu.negative_responses.get(nrc_val)
                nrc_desc = nrc_obj.short_name if nrc_obj else f"Unknown NRC (0x{nrc_val:02X})"
                result_data['nrc'] = {'code': nrc_val, 'description': nrc_desc}
                error_msg = f"NRC 0x{nrc_val:02X}: {nrc_desc}"
                self.progress.emit(self.network_id, error_msg)
                self.finished.emit(self.network_id, False, result_data, raw_request, raw_response)


        except Exception as e:
            error_details = traceback.format_exc()
            error_msg = f"Lỗi thực thi chẩn đoán:\n{e}\n\nChi tiết:\n{error_details}"
            self.progress.emit(self.network_id, "Lỗi chẩn đoán.")
            # Emit finished with success=False for general errors, pass the error string
            self.finished.emit(self.network_id, False, error_msg, raw_request, raw_response)
        finally:
            self._is_running = False
            if isotp_stack:
                try:
                    isotp_stack.stop()
                except Exception as cleanup_e:
                    print(f"Lỗi khi dừng isotp stack: {cleanup_e}")

    def stop(self):
        self.progress.emit(self.network_id, "Đang yêu cầu dừng tác vụ chẩn đoán...")
        self._is_running = False
        # Stopping isotp.recv() might be tricky, rely on timeout for now

    def isotp_error_handler(self, error):
        """Callback for errors detected by the isotp layer."""
        error_msg = f"Lỗi ISO-TP: {error}"
        self.progress.emit(self.network_id, error_msg)
        print(f"ISO-TP Error (Net: {self.network_id}): {error}")
        # Consider terminating the operation or signaling the main thread


# --- BASE TAB CLASS ---

class BaseNetworkTab(QWidget):
    """Lớp cơ sở cho các tab, chứa tham chiếu đến network_data hiện tại."""
    loadDbcRequested = pyqtSignal(str) # network_id
    loadTraceRequested = pyqtSignal(str) # network_id
    selectLogFileRequested = pyqtSignal(str) # network_id
    toggleLoggingRequested = pyqtSignal(str) # network_id
    # Add signals for diagnostics if needed here, or handle via main window

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_network_id = None
        # Store a reference to the main app's data for easier access if needed
        self._main_app_data_ref = parent.networks_data if parent and hasattr(parent, 'networks_data') else {}
        # Keep a local copy/reference of the specific network's data being displayed
        self.network_data = {}


    def update_content(self, network_id, network_data):
        """Cập nhật nội dung của tab dựa trên network được chọn."""
        self.current_network_id = network_id
        # Use a shallow copy to avoid modifying the main data structure directly
        # unless intended. For display, a reference might be fine.
        self.network_data = network_data
        # print(f"Tab {self.__class__.__name__} updated for network: {network_id}")

    # Helper to get main window reference if needed securely
    def get_main_window(self):
         parent = self.parent()
         while parent is not None:
              if isinstance(parent, MultiCanManagerApp): # Check if parent is the main window class
                   return parent
              parent = parent.parent()
         return None


# --- UI TAB CLASSES ---

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
        ]
        self.dbcStructureTree.setColumnCount(len(headers))
        self.dbcStructureTree.setHeaderLabels(headers)
        self.dbcStructureTree.header().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.dbcStructureTree.header().setSectionResizeMode(0, QHeaderView.Stretch) # Stretch Name column

    def _request_load_dbc(self):
        if self.current_network_id:
            self.loadDbcRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data):
        super().update_content(network_id, network_data)
        dbc_path = network_data.get('dbc_path', None)
        db = network_data.get('db', None)

        if dbc_path:
            self.dbcPathLabel.setText(f"File DBC: {os.path.basename(dbc_path)}")
            self.dbcPathLabel.setToolTip(dbc_path)
        else:
            self.dbcPathLabel.setText("File DBC: Chưa tải")
            self.dbcPathLabel.setToolTip("")

        self.dbcStructureTree.clear()
        if db:
            self.populate_dbc_tree(db)

    def populate_dbc_tree(self, db):
        self.dbcStructureTree.setUpdatesEnabled(False)
        try:
            node_items = {}
            for node in sorted(db.nodes, key=lambda n: n.name):
                node_item = QTreeWidgetItem(self.dbcStructureTree, [node.name])
                node_items[node.name] = node_item
                if node.comment: node_item.setText(12, node.comment)

            no_sender_item = None
            sorted_messages = sorted(db.messages, key=lambda m: m.frame_id)
            for msg in sorted_messages:
                parent_item = None
                senders_str = ", ".join(sorted(msg.senders)) if msg.senders else "N/A"
                if msg.senders:
                    first_sender = sorted(msg.senders)[0]
                    if first_sender in node_items: parent_item = node_items[first_sender]
                if not parent_item:
                    if not no_sender_item:
                        no_sender_item = QTreeWidgetItem(self.dbcStructureTree, ["[Không rõ Sender hoặc Sender không định nghĩa]"])
                        self.dbcStructureTree.insertTopLevelItem(0, no_sender_item)
                    parent_item = no_sender_item

                msg_data = [
                    f"{msg.name}", f"0x{msg.frame_id:X}", str(msg.length), senders_str,
                    "", "", "", "", "", "", "", "", msg.comment if msg.comment else ""
                ]
                message_item = QTreeWidgetItem(parent_item, msg_data)

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

            for item in node_items.values(): item.setExpanded(True)
            if no_sender_item: no_sender_item.setExpanded(True)

            for i in range(1, self.dbcStructureTree.columnCount()):
                self.dbcStructureTree.resizeColumnToContents(i)
        finally:
            self.dbcStructureTree.setUpdatesEnabled(True)

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
        self.traceTable.setWordWrap(False) # Performance for large tables
        self.traceTable.setVerticalScrollMode(QTableWidget.ScrollPerPixel)
        self.traceTable.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)
        # self.traceTable.setFont(QFont("Consolas", 9)) # Optional: Monospace font

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
             self.tracePathLabel.setToolTip(trace_path)
        else:
             self.tracePathLabel.setText("Trace: Chưa tải")
             self.tracePathLabel.setToolTip("")

        self.populate_trace_table(trace_data, db)

    def populate_trace_table(self, trace_data, db):
        self.traceTable.setUpdatesEnabled(False) # Tắt update để tăng tốc
        self.traceTable.setRowCount(0) # Xóa bảng cũ
        if not trace_data:
            self.traceTable.setUpdatesEnabled(True)
            return

        self.traceTable.setRowCount(len(trace_data))

        try:
            for row_idx, row_data in enumerate(trace_data):
                if len(row_data) < 4: continue # Skip invalid rows
                timestamp, id_hex, dlc, data_hex = row_data

                item_ts = QTableWidgetItem(timestamp)
                item_id = QTableWidgetItem(id_hex)
                item_dlc = QTableWidgetItem(dlc)
                item_data = QTableWidgetItem(data_hex)
                item_decoded = QTableWidgetItem("") # Reset cột giải mã

                item_id.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                item_dlc.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

                self.traceTable.setItem(row_idx, 0, item_ts)
                self.traceTable.setItem(row_idx, 1, item_id)
                self.traceTable.setItem(row_idx, 2, item_dlc)
                self.traceTable.setItem(row_idx, 3, item_data)
                self.traceTable.setItem(row_idx, 4, item_decoded)

                # --- Giải mã nếu có DBC ---
                decoded_str_list = []
                if db:
                    try:
                        can_id = int(id_hex, 16)
                        message = db.get_message_by_frame_id(can_id)
                        data_bytes = bytes.fromhex(data_hex)
                        decoded_signals = message.decode(data_bytes, decode_choices=False, allow_truncated=True)

                        for name, val in decoded_signals.items():
                             # Format value nicely
                             if isinstance(val, float): val_str = f"{val:.4g}"
                             elif isinstance(val, (int, bool)): val_str = str(val)
                             else: val_str = repr(val) # Handle strings, enums etc.
                             decoded_str_list.append(f"{name}={val_str}")

                        # Phát tín hiệu cập nhật giá trị signal cho tab khác
                        ts_obj = timestamp # Giữ nguyên dạng string hoặc parse nếu cần
                        for sig_name, sig_value in decoded_signals.items():
                            self.signalValueUpdate.emit(self.current_network_id, sig_name, sig_value, ts_obj)
                            # Don't store latest values here, main app should do it

                    except KeyError:
                        decoded_str_list.append("(ID không có trong DBC)")
                    except ValueError:
                        decoded_str_list.append("(Lỗi data hex)")
                    except Exception as e:
                        decoded_str_list.append(f"(Lỗi decode: {type(e).__name__})")

                decoded_full_str = "; ".join(decoded_str_list)
                if decoded_full_str:
                    item_decoded.setText(decoded_full_str)
                    item_decoded.setToolTip(decoded_full_str) # Tooltip hữu ích

        finally:
            self.traceTable.setUpdatesEnabled(True)
            # Tự động resize cột lần đầu (optional)
            # if not hasattr(self, '_trace_columns_resized'):
            #      self.traceTable.resizeColumnsToContents()
            #      self._trace_columns_resized = True

class SignalDataTab(BaseNetworkTab):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        # Optional Filter
        # ...

        self.signalTable = QTableWidget()
        self._setup_signal_table()
        layout.addWidget(self.signalTable)

        # Local cache of row mapping for faster updates
        self._signal_row_map = {}

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
        # Get latest values from the main data structure passed in
        latest_signal_values = network_data.get('latest_signal_values', {})

        self.populate_signal_list(db, latest_signal_values)

    def populate_signal_list(self, db, latest_signal_values):
        self.signalTable.setUpdatesEnabled(False)
        self.signalTable.setRowCount(0)
        self._signal_row_map.clear() # Clear local cache

        if not db:
            self.signalTable.setUpdatesEnabled(True)
            return

        signals_to_display = []
        try:
            for msg in db.messages:
                for sig in msg.signals:
                    signals_to_display.append(sig)

            signals_to_display.sort(key=lambda s: s.name) # Sắp xếp theo tên

            self.signalTable.setRowCount(len(signals_to_display))

            for row_idx, sig in enumerate(signals_to_display):
                sig_name = sig.name
                self._signal_row_map[sig_name] = row_idx # Update cache

                item_name = QTableWidgetItem(sig_name)
                item_value = QTableWidgetItem("") # Giá trị ban đầu trống
                item_unit = QTableWidgetItem(sig.unit if sig.unit else "")
                item_ts = QTableWidgetItem("")

                # Lấy giá trị mới nhất nếu có
                if sig_name in latest_signal_values:
                    value, timestamp = latest_signal_values[sig_name]
                    value_str = f"{value:.4g}" if isinstance(value, (float, int)) else str(value)
                    item_value.setText(value_str)
                    item_ts.setText(str(timestamp))

                self.signalTable.setItem(row_idx, 0, item_name)
                self.signalTable.setItem(row_idx, 1, item_value)
                self.signalTable.setItem(row_idx, 2, item_unit)
                self.signalTable.setItem(row_idx, 3, item_ts)

        finally:
            self.signalTable.setUpdatesEnabled(True)
            # Optional: Resize columns
            # self.signalTable.resizeColumnsToContents()

    def update_signal_value(self, signal_name, value, timestamp):
        """Slot để cập nhật một giá trị signal trong bảng."""
        if signal_name in self._signal_row_map:
            row_idx = self._signal_row_map[signal_name]

            # Check if row index is still valid (table might have changed)
            if row_idx >= self.signalTable.rowCount():
                 # print(f"Warning: Row index {row_idx} for signal {signal_name} out of bounds.")
                 # Optionally rebuild map or ignore update
                 return

            value_str = f"{value:.4g}" if isinstance(value, (float, int)) else str(value)
            ts_str = str(timestamp)

            # Cập nhật item nếu nó tồn tại, tạo mới nếu không (ít xảy ra)
            value_item = self.signalTable.item(row_idx, 1)
            if value_item: value_item.setText(value_str)
            else: self.signalTable.setItem(row_idx, 1, QTableWidgetItem(value_str))

            ts_item = self.signalTable.item(row_idx, 3)
            if ts_item: ts_item.setText(ts_str)
            else: self.signalTable.setItem(row_idx, 3, QTableWidgetItem(ts_str))
        # else:
            # print(f"Signal {signal_name} not found in table map for update") # Debug

class GraphingTab(BaseNetworkTab):
     def __init__(self, parent=None):
          super().__init__(parent)
          layout = QVBoxLayout(self)
          layout.setContentsMargins(5, 5, 5, 5)

          self.plot_items = {} # Store plot data items for updates {sig_name: plotDataItem}

          if PYQTGRAPH_AVAILABLE:
                graph_splitter = QSplitter(Qt.Horizontal)

                # Controls Panel
                control_widget = QWidget()
                control_layout = QVBoxLayout(control_widget)
                control_layout.addWidget(QLabel("Chọn Tín hiệu để Vẽ:"))
                self.signalListWidget = QListWidget()
                self.signalListWidget.setSelectionMode(QListWidget.ExtendedSelection)
                self.signalListWidget.itemSelectionChanged.connect(self._plot_selected_signals)
                control_layout.addWidget(self.signalListWidget)
                control_widget.setMaximumWidth(300)

                # Plot Panel
                pg.setConfigOption('background', 'w') # Set background before creating plot
                pg.setConfigOption('foreground', 'k')
                self.plotWidget = pg.PlotWidget(name="Signal Plot") # Give it a name
                self.plotWidget.showGrid(x=True, y=True, alpha=0.3)
                self.plotWidget.addLegend(offset=(-10, 10)) # Adjust legend position

                graph_splitter.addWidget(control_widget)
                graph_splitter.addWidget(self.plotWidget)
                graph_splitter.setSizes([200, 600])

                layout.addWidget(graph_splitter)

          else:
                layout.addWidget(QLabel("Thư viện 'pyqtgraph' không có sẵn. Không thể hiển thị đồ thị."))

          # Local cache of time series data for performance
          self._local_signal_time_series = {}

     def update_content(self, network_id, network_data):
          super().update_content(network_id, network_data)
          # Make a shallow copy to avoid modifying the original if we filter/process locally
          self._local_signal_time_series = network_data.get('signal_time_series', {}).copy()
          db = network_data.get('db', None)

          if PYQTGRAPH_AVAILABLE:
                # Clear existing plots and legend
                self.plotWidget.clear()
                self.plot_items.clear()
                # Check if legend exists before removing
                if self.plotWidget.plotItem.legend:
                    self.plotWidget.plotItem.legend.scene().removeItem(self.plotWidget.plotItem.legend)
                    # Create a fresh legend
                    self.plotWidget.addLegend(offset=(-10, 10))


                # Clear and repopulate the signal list
                selected_signal_names = {item.text() for item in self.signalListWidget.selectedItems()}
                self.signalListWidget.clear()

                signals_with_data = []
                if db:
                     all_signal_names = sorted([sig.name for msg in db.messages for sig in msg.signals])
                     # Only list signals that actually have timeseries data
                     signals_with_data = [name for name in all_signal_names if name in self._local_signal_time_series and self._local_signal_time_series[name][0]]
                     self.signalListWidget.addItems(signals_with_data)

                     # Restore selection
                     self.signalListWidget.blockSignals(True)
                     for i in range(self.signalListWidget.count()):
                          item = self.signalListWidget.item(i)
                          if item.text() in selected_signal_names:
                               item.setSelected(True)
                     self.signalListWidget.blockSignals(False)

                print(f"Graph tab updated for {network_id}. Signals with data: {len(signals_with_data)}")
                # Re-plot selected signals after update
                self._plot_selected_signals()

          # else: handle case where pyqtgraph is not available

     def _plot_selected_signals(self):
          if not PYQTGRAPH_AVAILABLE: return

          # Clear plots but keep legend items associated with self.plot_items
          for item in self.plot_items.values():
               self.plotWidget.removeItem(item)
          self.plot_items.clear()

          selected_items = self.signalListWidget.selectedItems()
          if not selected_items:
               return # Nothing to plot

          # Define a list of distinct colors
          pens = [pg.mkPen(color=c, width=1.5) for c in ['#0072B2', '#D55E00', '#009E73', '#CC79A7', '#56B4E9', '#E69F00', '#F0E442', 'k'] * 3] # Colorblind-friendly palette + black, repeated


          plot_count = 0
          max_points_display = 50000 # Limit points for performance

          for idx, item in enumerate(selected_items):
                sig_name = item.text()
                if sig_name in self._local_signal_time_series:
                     timestamps, values = self._local_signal_time_series[sig_name]

                     if timestamps and values and len(timestamps) == len(values):
                          # Filter numeric data for plotting
                          numeric_timestamps = []
                          numeric_values = []
                          valid_data = False
                          for t, v in zip(timestamps, values):
                              # Ensure timestamp is numeric
                              if isinstance(t, (int, float)):
                                  # Ensure value is numeric (or can be converted, e.g., boolean to 0/1)
                                   if isinstance(v, (int, float)):
                                        numeric_timestamps.append(t)
                                        numeric_values.append(v)
                                        valid_data = True
                                   elif isinstance(v, bool):
                                        numeric_timestamps.append(t)
                                        numeric_values.append(int(v)) # Plot bools as 0 or 1
                                        valid_data = True
                                   # Add handling for other types if needed (e.g., enums mapped to ints)

                          if valid_data:
                               pen = pens[plot_count % len(pens)]
                               plot_name = sig_name
                               # Sample data if too large
                               if len(numeric_timestamps) > max_points_display:
                                    step = len(numeric_timestamps) // max_points_display
                                    sampled_ts = numeric_timestamps[::step]
                                    sampled_val = numeric_values[::step]
                                    plot_data_item = self.plotWidget.plot(sampled_ts, sampled_val, pen=pen, name=plot_name)
                                    # print(f"Plotting {sig_name} (Sampled {len(sampled_ts)} points)")
                               else:
                                    plot_data_item = self.plotWidget.plot(numeric_timestamps, numeric_values, pen=pen, name=plot_name)
                                    # print(f"Plotting {sig_name} ({len(numeric_timestamps)} points)")

                               self.plot_items[sig_name] = plot_data_item # Store reference to the plot item
                               plot_count += 1
                          # else: print(f"Signal '{sig_name}' has no numeric data to plot.")
                     # else: print(f"Timeseries data for '{sig_name}' is invalid or empty.")
                # else: print(f"Timeseries data for '{sig_name}' not found in local cache.")

          # Auto-range axes after plotting
          self.plotWidget.autoRange()

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
             # Check the main data structure for the log path
             main_window = self.get_main_window()
             if main_window and self.current_network_id in main_window.networks_data:
                  current_log_path = main_window.networks_data[self.current_network_id].get('log_path')
                  if checked and not current_log_path:
                       QMessageBox.warning(self, "Thiếu File Log", "Vui lòng chọn file log trước khi bắt đầu ghi.")
                       # Force button state back without emitting signal again
                       self.toggleLogButton.blockSignals(True)
                       self.toggleLogButton.setChecked(False)
                       self.toggleLogButton.blockSignals(False)
                       return
             elif checked: # Cannot check main data, prevent starting without path
                  QMessageBox.warning(self, "Lỗi", "Không thể xác minh đường dẫn file log.")
                  self.toggleLogButton.blockSignals(True)
                  self.toggleLogButton.setChecked(False)
                  self.toggleLogButton.blockSignals(False)
                  return

             self.toggleLoggingRequested.emit(self.current_network_id)


    def update_content(self, network_id, network_data):
        super().update_content(network_id, network_data)
        is_logging = network_data.get('is_logging', False)
        log_path = network_data.get('log_path', None)

        self.logPathEdit.setText(log_path if log_path else "")
        self.logPathEdit.setToolTip(log_path if log_path else "")

        # Cập nhật trạng thái nút và label (tránh trigger tín hiệu toggled)
        was_blocked = self.toggleLogButton.blockSignals(True)
        self.toggleLogButton.setChecked(is_logging)
        if is_logging:
            log_filename = os.path.basename(log_path) if log_path else "N/A"
            self.statusLabel.setText(f"Trạng thái Log: Đang ghi vào {log_filename}")
            self.toggleLogButton.setText("Dừng Ghi")
            self.toggleLogButton.setIcon(QIcon.fromTheme("media-playback-stop"))
        else:
            self.statusLabel.setText("Trạng thái Log: Đang dừng")
            self.toggleLogButton.setText("Bắt đầu Ghi")
            self.toggleLogButton.setIcon(QIcon.fromTheme("media-record"))
        self.toggleLogButton.blockSignals(was_blocked)

# --- DIAGNOSTICS TAB UI ---

class DiagnosticsTab(BaseNetworkTab):
    """Tab for performing ODX/PDX based diagnostics."""
    loadDiagFileRequested = pyqtSignal(str) # network_id
    # Signal to trigger diagnostic action in main window
    # Args: network_id, request_details_dict
    diagnosticActionRequested = pyqtSignal(str, dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.odx_database = None
        self.selected_ecu_diag_layer = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        # --- Top Configuration Area ---
        config_group = QGroupBox("Cấu hình Chẩn đoán")
        config_layout = QVBoxLayout(config_group)

        # File Loading
        file_layout = QHBoxLayout()
        self.diagFilePathLabel = QLabel("File PDX/ODX/CDD: Chưa tải")
        self.diagFilePathLabel.setWordWrap(True)
        self.loadDiagFileButton = QPushButton("Tải File PDX/ODX...") # CDD support is placeholder
        self.loadDiagFileButton.clicked.connect(self._request_load_diag_file)
        file_layout.addWidget(self.diagFilePathLabel, 1)
        file_layout.addWidget(self.loadDiagFileButton)
        config_layout.addLayout(file_layout)

        # ECU Selection
        ecu_layout = QHBoxLayout()
        ecu_layout.addWidget(QLabel("Chọn ECU/Variant:"))
        self.ecuSelectComboBox = QComboBox()
        self.ecuSelectComboBox.currentIndexChanged.connect(self.on_ecu_selected)
        ecu_layout.addWidget(self.ecuSelectComboBox, 1)
        config_layout.addLayout(ecu_layout)

        layout.addWidget(config_group)

        # --- Main Interaction Area (Splitter) ---
        diag_splitter = QSplitter(Qt.Vertical)

        # --- Service Execution Area ---
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        self.service_widget = QWidget() # Renamed from service_widget to self.service_widget
        self.service_layout = QVBoxLayout(self.service_widget) # Add service groups here
        self.service_layout.setAlignment(Qt.AlignTop)
        scroll_area.setWidget(self.service_widget)
        diag_splitter.addWidget(scroll_area)

        # --- Results Area ---
        results_group = QGroupBox("Kết quả và Log")
        results_layout = QVBoxLayout(results_group)
        results_splitter = QSplitter(Qt.Vertical)

        # Decoded Results Display
        self.resultsTextEdit = QTextEdit()
        self.resultsTextEdit.setReadOnly(True)
        self.resultsTextEdit.setFontFamily("Consolas") # Monospace helpful
        results_splitter.addWidget(self.resultsTextEdit)

        # Raw Request/Response Log
        self.rawLogTextEdit = QTextEdit()
        self.rawLogTextEdit.setReadOnly(True)
        self.rawLogTextEdit.setFontFamily("Consolas")
        results_splitter.addWidget(self.rawLogTextEdit)
        results_splitter.setSizes([200, 100]) # Initial sizes

        results_layout.addWidget(results_splitter)
        diag_splitter.addWidget(results_group)

        diag_splitter.setSizes([400, 300]) # Adjust initial sizes as needed
        layout.addWidget(diag_splitter, 1) # Make splitter take available space

        # --- Populate Services (Example) ---
        self._setup_service_widgets()

        # Initially disable ECU selection and services
        self.ecuSelectComboBox.setEnabled(False)
        self.service_widget.setEnabled(False) # Disable service area initially

    def _setup_service_widgets(self):
        """Create UI elements for common diagnostic services."""
        # --- Read DID (0x22) ---
        did_group = QGroupBox("Read Data By Identifier (0x22 / ReadDID)")
        did_layout = QVBoxLayout(did_group)
        did_select_layout = QHBoxLayout()
        did_select_layout.addWidget(QLabel("Chọn DID:"))
        self.didComboBox = QComboBox()
        did_select_layout.addWidget(self.didComboBox, 1)
        read_did_button = QPushButton("Đọc DID")
        read_did_button.clicked.connect(self.on_read_did_clicked)
        did_select_layout.addWidget(read_did_button)
        did_layout.addLayout(did_select_layout)
        self.service_layout.addWidget(did_group)

        # --- Read DTC (0x19) ---
        dtc_group = QGroupBox("Read DTC Information (0x19 / ReadDTC)")
        dtc_layout = QVBoxLayout(dtc_group)
        dtc_select_layout = QHBoxLayout()
        dtc_select_layout.addWidget(QLabel("Loại báo cáo:"))
        self.dtcSubfunctionCombo = QComboBox()
        # Populate with common subfunctions (validated against ODX later)
        self.dtcSubfunctionCombo.addItem("reportNumberOfDTCByStatusMask (0x01)", 0x01)
        self.dtcSubfunctionCombo.addItem("reportDTCByStatusMask (0x02)", 0x02)
        self.dtcSubfunctionCombo.addItem("reportDTCSnapshotIdentification (0x03)", 0x03)
        self.dtcSubfunctionCombo.addItem("reportDTCSnapshotRecordByDTCNumber (0x04)", 0x04)
        self.dtcSubfunctionCombo.addItem("reportDTCStoredDataByRecordNumber (0x06)", 0x06)
        self.dtcSubfunctionCombo.addItem("reportUserDefMemoryDTCByStatusMask (0x17)", 0x17) # Example
        # ... Add others
        dtc_select_layout.addWidget(self.dtcSubfunctionCombo, 1)
        # Optional Status Mask Input (Maybe enable based on subfunction)
        self.dtcStatusMaskEdit = QLineEdit("FF")
        self.dtcStatusMaskEdit.setFixedWidth(50)
        self.dtcStatusMaskEdit.setToolTip("Status Mask (Hex, e.g., FF)")
        dtc_select_layout.addWidget(QLabel("Mask:"))
        dtc_select_layout.addWidget(self.dtcStatusMaskEdit)

        read_dtc_button = QPushButton("Đọc DTC")
        read_dtc_button.clicked.connect(self.on_read_dtc_clicked)
        dtc_select_layout.addWidget(read_dtc_button)
        dtc_layout.addLayout(dtc_select_layout)
        self.service_layout.addWidget(dtc_group)

        # --- Clear DTC (0x14) ---
        clear_dtc_group = QGroupBox("Clear Diagnostic Information (0x14 / ClearDTC)")
        clear_dtc_layout = QHBoxLayout(clear_dtc_group)
        clear_dtc_layout.addWidget(QLabel("Group Of DTC (Hex):"))
        self.clearDtcGroupEdit = QLineEdit("FFFFFF") # Default: all DTCs
        clear_dtc_layout.addWidget(self.clearDtcGroupEdit)
        clear_dtc_button = QPushButton("Xóa DTC")
        clear_dtc_button.clicked.connect(self.on_clear_dtc_clicked)
        clear_dtc_layout.addWidget(clear_dtc_button)
        clear_dtc_layout.addStretch(1)
        self.service_layout.addWidget(clear_dtc_group)

        # --- ECU Reset (0x11) ---
        reset_group = QGroupBox("ECU Reset (0x11)")
        reset_layout = QHBoxLayout(reset_group)
        reset_layout.addWidget(QLabel("Loại Reset:"))
        self.resetTypeComboBox = QComboBox()
        # Populate from ODX ideally, or use common values
        self.resetTypeComboBox.addItem("hardReset (0x01)", 0x01)
        self.resetTypeComboBox.addItem("keyOffOnReset (0x02)", 0x02)
        self.resetTypeComboBox.addItem("softReset (0x03)", 0x03)
        self.resetTypeComboBox.addItem("enableRapidPowerShutDown (0x04)", 0x04)
        self.resetTypeComboBox.addItem("disableRapidPowerShutDown (0x05)", 0x05)
        reset_layout.addWidget(self.resetTypeComboBox, 1)
        reset_button = QPushButton("Reset ECU")
        reset_button.clicked.connect(self.on_reset_ecu_clicked)
        reset_layout.addWidget(reset_button)
        self.service_layout.addWidget(reset_group)

        # --- Security Access (0x27) ---
        sec_access_group = QGroupBox("Security Access (0x27) - Logic phức tạp")
        sec_layout = QGridLayout(sec_access_group) # Use grid for better layout
        sec_layout.addWidget(QLabel("Cấp độ (Hex):"), 0, 0)
        self.securityLevelCombo = QComboBox() # Populate with levels from ODX
        sec_layout.addWidget(self.securityLevelCombo, 0, 1)
        sec_layout.addWidget(QLabel("Seed (Read Only):"), 1, 0)
        self.seedLineEdit = QLineEdit()
        self.seedLineEdit.setReadOnly(True)
        sec_layout.addWidget(self.seedLineEdit, 1, 1)
        sec_layout.addWidget(QLabel("Key (Input):"), 2, 0)
        self.keyLineEdit = QLineEdit()
        sec_layout.addWidget(self.keyLineEdit, 2, 1)
        request_seed_button = QPushButton("Yêu cầu Seed")
        request_seed_button.clicked.connect(self.on_request_seed_clicked)
        sec_layout.addWidget(request_seed_button, 3, 0)
        send_key_button = QPushButton("Gửi Key")
        send_key_button.clicked.connect(self.on_send_key_clicked)
        sec_layout.addWidget(send_key_button, 3, 1)
        # Disable initially, requires specific logic
        sec_access_group.setEnabled(False) # TODO: Implement Security Access logic
        self.service_layout.addWidget(sec_access_group)

        # --- Write DID (0x2E) ---
        write_did_group = QGroupBox("Write Data By Identifier (0x2E) - Cần mã hóa")
        write_layout = QGridLayout(write_did_group)
        write_layout.addWidget(QLabel("Chọn DID:"), 0, 0)
        self.writeDidComboBox = QComboBox() # Populate with writable DIDs from ODX
        write_layout.addWidget(self.writeDidComboBox, 0, 1)
        write_layout.addWidget(QLabel("Dữ liệu (Hex):"), 1, 0) # Input as Hex for now
        self.writeDataLineEdit = QLineEdit()
        self.writeDataLineEdit.setPlaceholderText("Nhập dữ liệu dạng hex (ví dụ: 01AF)...")
        write_layout.addWidget(self.writeDataLineEdit, 1, 1)
        write_did_button = QPushButton("Ghi DID")
        write_did_button.clicked.connect(self.on_write_did_clicked)
        write_layout.addWidget(write_did_button, 2, 1)
        # Disable initially, requires specific logic
        write_did_group.setEnabled(False) # TODO: Implement Write DID logic (encoding)
        self.service_layout.addWidget(write_did_group)

        self.service_layout.addStretch(1) # Push services upwards

    def set_service_controls_enabled(self, enabled):
        """Enable/disable all service group boxes."""
        self.service_widget.setEnabled(enabled)
        # Optionally disable/enable specific buttons during execution

    def _request_load_diag_file(self):
        if self.current_network_id:
            self.loadDiagFileRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data):
        """Cập nhật tab khi mạng được chọn hoặc dữ liệu thay đổi."""
        super().update_content(network_id, network_data)
        self.odx_database = network_data.get('odx_database', None)
        diag_path = network_data.get('diag_file_path', None)
        # Get the currently selected ECU DiagLayer object stored in main data
        self.selected_ecu_diag_layer = network_data.get('selected_ecu_diag_layer', None)

        # Update File Path Label
        if diag_path:
            self.diagFilePathLabel.setText(f"File Diag: {os.path.basename(diag_path)}")
            self.diagFilePathLabel.setToolTip(diag_path)
        else:
            self.diagFilePathLabel.setText("File PDX/ODX/CDD: Chưa tải")
            self.diagFilePathLabel.setToolTip("")

        # --- Block signals while repopulating ---
        self.ecuSelectComboBox.blockSignals(True)
        self.didComboBox.blockSignals(True)
        self.writeDidComboBox.blockSignals(True)
        self.securityLevelCombo.blockSignals(True)
        # ... block others ...

        # --- Clear previous state ---
        self.ecuSelectComboBox.clear()
        self.didComboBox.clear()
        self.writeDidComboBox.clear()
        self.securityLevelCombo.clear()
        self.seedLineEdit.clear()
        self.keyLineEdit.clear()
        self.resultsTextEdit.clear() # Clear results on network change/ODX reload
        self.rawLogTextEdit.clear()

        # --- Enable/Disable based on ODX loaded state ---
        if self.odx_database and isinstance(self.odx_database, odxtools.Database):
            self.ecuSelectComboBox.setEnabled(True)

            # --- Populate ECU ComboBox ---
            current_ecu_index = -1
            ecu_layers = list(self.odx_database.diag_layers)
            ecu_layers.sort(key=lambda ecu: ecu.short_name) # Sort alphabetically

            for i, ecu in enumerate(ecu_layers):
                 display_name = f"{ecu.short_name} ({ecu.variant_type})"
                 self.ecuSelectComboBox.addItem(display_name, ecu) # Store DiagLayer object
                 # Check if this ECU matches the one stored in network_data
                 if self.selected_ecu_diag_layer and ecu == self.selected_ecu_diag_layer:
                     current_ecu_index = i

            # --- Restore ECU selection ---
            if current_ecu_index != -1:
                 self.ecuSelectComboBox.setCurrentIndex(current_ecu_index)
                 # Manually trigger update for the restored ECU if index > -1
                 self.on_ecu_selected(current_ecu_index)
            elif self.ecuSelectComboBox.count() > 0:
                 # If no previous selection or it wasn't found, select the first ECU
                 self.ecuSelectComboBox.setCurrentIndex(0)
                 # Manually trigger update for the first ECU
                 self.on_ecu_selected(0)
            else:
                 # No ECUs found in the ODX database
                 self.selected_ecu_diag_layer = None # Ensure selection is cleared
                 # Store cleared selection back to main data
                 main_window = self.get_main_window()
                 if main_window and self.current_network_id in main_window.networks_data:
                      main_window.networks_data[self.current_network_id]['selected_ecu_diag_layer'] = None
                 self.service_widget.setEnabled(False) # Disable services

        else:
            # No ODX loaded
            self.ecuSelectComboBox.setEnabled(False)
            self.service_widget.setEnabled(False)
            self.selected_ecu_diag_layer = None # Clear selection

        # --- Unblock signals ---
        self.ecuSelectComboBox.blockSignals(False)
        self.didComboBox.blockSignals(False)
        self.writeDidComboBox.blockSignals(False)
        self.securityLevelCombo.blockSignals(False)
        # ... unblock others ...

    def on_ecu_selected(self, index):
        """Cập nhật UI khi người dùng chọn một ECU/Variant khác."""
        main_window = self.get_main_window()
        if not main_window or not self.current_network_id or self.current_network_id not in main_window.networks_data:
             print("Lỗi: Không tìm thấy main window hoặc network data khi chọn ECU.")
             return

        if index < 0: # Should not happen if populated, but safety check
            self.selected_ecu_diag_layer = None
            main_window.networks_data[self.current_network_id]['selected_ecu_diag_layer'] = None
            self.service_widget.setEnabled(False)
            # Clear dependent combos
            self.didComboBox.clear()
            self.writeDidComboBox.clear()
            self.securityLevelCombo.clear()
            return

        # Get the selected DiagLayer object from the ComboBox data
        self.selected_ecu_diag_layer = self.ecuSelectComboBox.itemData(index)

        # --- Store the selection back in the main network_data structure ---
        main_window.networks_data[self.current_network_id]['selected_ecu_diag_layer'] = self.selected_ecu_diag_layer
        print(f"Net {self.current_network_id}: Selected ECU - {self.selected_ecu_diag_layer.short_name}")

        # Enable the service area now that an ECU is selected
        self.service_widget.setEnabled(True)
        self.clear_results() # Clear results when ECU changes

        # --- Populate dependent UI elements (DIDs, Routines, Security Levels, etc.) ---
        self._populate_dids()
        self._populate_writable_dids()
        self._populate_security_levels()
        # TODO: Populate other elements like Routines if added

    def _populate_dids(self):
        """Populates the Read DID ComboBox based on the selected ECU."""
        self.didComboBox.blockSignals(True)
        self.didComboBox.clear()
        if not self.selected_ecu_diag_layer or not self.odx_database:
            self.didComboBox.blockSignals(False)
            return

        dids = [] # List of tuples: (display_name, did_integer_value)
        try:
            # Find DIDs associated with the ReadDataByIdentifier service
            # This often involves looking at DataObjectProperties (DOPs)
            read_did_service = self.selected_ecu_diag_layer.services.get("ReadDataByIdentifier")
            if read_did_service and read_did_service.positive_responses:
                 # Look through positive responses for parameters linked to DOPs
                 for resp in read_did_service.positive_responses:
                      for param in resp.parameters:
                           # Heuristic: Check if parameter name suggests it holds the DID value list
                           # Or check if it references a structure/DOP containing DIDs
                           # This part is highly ODX-specific!
                           # A common pattern is a DOP for the 'ListOfIdentifiers' parameter
                           if param.dop_ref:
                                dop = self.odx_database.data_object_properties.get(param.dop_ref.id_ref)
                                if dop:
                                     # Check if this DOP or its components define constants that look like DIDs
                                     # Example: Check diag_coded_type or physical_type constraints
                                     # This needs refinement based on odxtools API and ODX structure
                                     pass # Add logic here

            # Fallback/Alternative: Search all DOPs for potential DIDs
            # This might list things that aren't readable via 0x22, but is a starting point
            if not dids:
                for dop_id, dop in self.odx_database.data_object_properties.items():
                     # Heuristic: Check for coded constant or specific types often used for DIDs
                     did_val = None
                     if dop.coded_constant is not None and isinstance(dop.coded_constant, int):
                          did_val = dop.coded_constant
                     # Add other checks, e.g., based on physical type name patterns

                     if did_val is not None and 0 <= did_val <= 0xFFFF: # Plausible DID range
                          display_name = f"{dop.short_name} (0x{did_val:04X})"
                          # Avoid duplicates
                          if not any(d[1] == did_val for d in dids):
                              dids.append((display_name, did_val))

            # Sort DIDs by value
            dids.sort(key=lambda x: x[1])

            if dids:
                for name, val in dids:
                    self.didComboBox.addItem(name, val)
            else:
                self.didComboBox.addItem("Không tìm thấy DID từ ODX", -1)

        except Exception as e:
             print(f"Lỗi khi tìm DIDs: {e}\n{traceback.format_exc()}")
             self.didComboBox.addItem("Lỗi tìm DID", -1)
        finally:
            self.didComboBox.blockSignals(False)

    def _populate_writable_dids(self):
        """Populates the Write DID ComboBox."""
        self.writeDidComboBox.blockSignals(True)
        self.writeDidComboBox.clear()
        # TODO: Implement logic similar to _populate_dids, but check if DIDs
        # are associated with the WriteDataByIdentifier service or marked as writable.
        # This requires more detailed ODX parsing.
        self.writeDidComboBox.addItem("Chưa hỗ trợ", -1) # Placeholder
        # Example (if logic was implemented):
        # writable_dids = self._find_dids_for_service("WriteDataByIdentifier")
        # if writable_dids:
        #     for name, val in writable_dids:
        #         self.writeDidComboBox.addItem(name, val)
        # else:
        #      self.writeDidComboBox.addItem("Không tìm thấy DID ghi được", -1)
        self.writeDidComboBox.blockSignals(False)


    def _populate_security_levels(self):
        """Populates the Security Access Level ComboBox."""
        self.securityLevelCombo.blockSignals(True)
        self.securityLevelCombo.clear()
        if not self.selected_ecu_diag_layer:
            self.securityLevelCombo.blockSignals(False)
            return

        levels = [] # List of tuples: (display_name, level_integer_value)
        try:
            sec_access_service = self.selected_ecu_diag_layer.services.get("SecurityAccess")
            if sec_access_service and sec_access_service.request:
                 # Assume the first parameter is the security level sub-function
                 level_param = sec_access_service.request.parameters[0]
                 # Find the data type associated with the level parameter
                 level_dt = None
                 if level_param.diag_coded_type_ref:
                     level_dt = self.odx_database.diag_coded_types.get(level_param.diag_coded_type_ref.id_ref)
                 # Alternative: Check DOP reference if used

                 if level_dt and hasattr(level_dt, 'bit_length'): # Check if it's a type we can analyze
                      # Check if the type uses COMPU-METHOD with VT (Value Text) pairs
                      if level_dt.compu_method and level_dt.compu_method.compu_internal_to_phys:
                         for scale in level_dt.compu_method.compu_internal_to_phys.compu_scales:
                              if scale.compu_const: # Look for VT pairs (text + value)
                                  level_val_int = int(scale.lower_limit) # Assuming lower_limit holds the value
                                  level_text = scale.compu_const.vt
                                  # Often levels are odd numbers for requestSeed, even for sendKey
                                  # Filter for requestSeed levels (usually odd)
                                  if level_val_int % 2 != 0:
                                       display_name = f"{level_text} (0x{level_val_int:02X})"
                                       levels.append((display_name, level_val_int))

            # Sort levels
            levels.sort(key=lambda x: x[1])

            if levels:
                for name, val in levels:
                    self.securityLevelCombo.addItem(name, val)
            else:
                 self.securityLevelCombo.addItem("Không tìm thấy cấp độ", -1)

        except Exception as e:
             print(f"Lỗi khi tìm Security Levels: {e}\n{traceback.format_exc()}")
             self.securityLevelCombo.addItem("Lỗi tìm cấp độ", -1)
        finally:
             self.securityLevelCombo.blockSignals(False)


    # --- Service Button Click Handlers ---

    def _emit_diagnostic_action(self, request_details):
        """Helper to emit the signal, checking network ID and ECU selection."""
        if not self.current_network_id:
            QMessageBox.warning(self, "Lỗi Mạng", "Không có mạng nào được chọn.")
            return False
        if not self.selected_ecu_diag_layer:
            QMessageBox.warning(self, "Lỗi ECU", "Chưa chọn ECU/Variant từ file ODX.")
            return False

        # --- Check Connection Status (using main window's data) ---
        main_window = self.get_main_window()
        if not main_window or self.current_network_id not in main_window.networks_data:
             QMessageBox.critical(self, "Lỗi", "Không thể truy cập dữ liệu mạng.")
             return False # Cannot proceed

        network_info = main_window.networks_data[self.current_network_id]
        if not network_info.get('is_connected', False):
           QMessageBox.warning(self, "Chưa Kết Nối", f"Mạng '{network_info.get('name', self.current_network_id)}' chưa được kết nối. Vui lòng kết nối phần cứng.")
           return False # Cannot proceed


        # --- Check if another diagnostic action is running for this network ---
        worker_id = f"{self.current_network_id}_diagaction"
        if worker_id in main_window.workers and main_window.workers[worker_id].isRunning():
             QMessageBox.information(self, "Đang xử lý", f"Đang thực hiện tác vụ chẩn đoán khác cho mạng này. Vui lòng đợi.")
             return False # Cannot start a new one


        # If all checks pass, clear results and emit the request
        self.clear_results()
        self.append_to_raw_log(f"--- Bắt đầu yêu cầu: {request_details.get('type', 'N/A')} ---")
        self.diagnosticActionRequested.emit(self.current_network_id, request_details)
        return True # Request emitted successfully

    def on_read_did_clicked(self):
        if self.didComboBox.currentIndex() < 0:
            QMessageBox.warning(self, "Thiếu thông tin", "Vui lòng chọn một DID.")
            return
        did_value = self.didComboBox.currentData()
        if did_value is None or not isinstance(did_value, int) or did_value < 0:
             QMessageBox.warning(self, "Lỗi DID", "Giá trị DID đã chọn không hợp lệ.")
             return

        details = {'type': 'read_did', 'did': did_value}
        self._emit_diagnostic_action(details)

    def on_read_dtc_clicked(self):
        if self.dtcSubfunctionCombo.currentIndex() < 0:
            QMessageBox.warning(self, "Thiếu thông tin", "Vui lòng chọn loại báo cáo DTC.")
            return
        subfunction = self.dtcSubfunctionCombo.currentData()
        details = {'type': 'read_dtc', 'subfunction': subfunction}

        # Include status mask if the subfunction requires it (e.g., 0x01, 0x02, 0x17)
        if subfunction in [0x01, 0x02, 0x17]: # Add other subfunctions needing mask
            mask_str = self.dtcStatusMaskEdit.text().strip()
            try:
                mask_int = int(mask_str, 16)
                if not (0 <= mask_int <= 0xFF): raise ValueError("Mask out of range")
                details['mask'] = mask_int
            except ValueError:
                QMessageBox.warning(self, "Lỗi Input", f"DTC Status Mask '{mask_str}' không hợp lệ (phải là Hex 00-FF).")
                return

        self._emit_diagnostic_action(details)

    def on_clear_dtc_clicked(self):
        group_str = self.clearDtcGroupEdit.text().strip()
        try:
            group_int = int(group_str, 16)
            if not (0 <= group_int <= 0xFFFFFF): raise ValueError("Group out of range")
        except ValueError:
            QMessageBox.warning(self, "Lỗi Input", f"Group Of DTC '{group_str}' không hợp lệ (phải là Hex 000000-FFFFFF).")
            return
        details = {'type': 'clear_dtc', 'group': group_int}
        self._emit_diagnostic_action(details)

    def on_reset_ecu_clicked(self):
         if self.resetTypeComboBox.currentIndex() < 0:
            QMessageBox.warning(self, "Thiếu thông tin", "Vui lòng chọn loại reset.")
            return
         reset_type = self.resetTypeComboBox.currentData()
         details = {'type': 'ecu_reset', 'subfunction': reset_type}
         self._emit_diagnostic_action(details)

    def on_request_seed_clicked(self):
         # TODO: Implement Security Access - Step 1: Request Seed
         QMessageBox.information(self, "Chưa hoàn thiện", "Chức năng Yêu cầu Seed chưa được triển khai.")
         # if self.securityLevelCombo.currentIndex() < 0: return
         # level = self.securityLevelCombo.currentData() # Get requestSeed level (odd number)
         # if level % 2 == 0: level += 1 # Ensure it's the request level if user selected sendKey level
         # details = {'type': 'security_access', 'subfunction': level}
         # self._emit_diagnostic_action(details)

    def on_send_key_clicked(self):
         # TODO: Implement Security Access - Step 2: Send Key
         QMessageBox.information(self, "Chưa hoàn thiện", "Chức năng Gửi Key chưa được triển khai.")
         # if self.securityLevelCombo.currentIndex() < 0: return
         # level = self.securityLevelCombo.currentData() # Get requestSeed level
         # if level % 2 != 0: level += 1 # Get sendKey level (even number)
         # key_str = self.keyLineEdit.text().strip()
         # try: key_bytes = bytes.fromhex(key_str)
         # except ValueError: QMessageBox.warning(self, "Lỗi", "Key phải là chuỗi Hex."); return
         # details = {'type': 'security_access', 'subfunction': level, 'key': key_bytes}
         # self._emit_diagnostic_action(details)

    def on_write_did_clicked(self):
         # TODO: Implement Write DID
         QMessageBox.information(self, "Chưa hoàn thiện", "Chức năng Ghi DID chưa được triển khai.")
         # if self.writeDidComboBox.currentIndex() < 0: return
         # did_value = self.writeDidComboBox.currentData()
         # data_str = self.writeDataLineEdit.text().strip()
         # try: data_bytes = bytes.fromhex(data_str)
         # except ValueError: QMessageBox.warning(self, "Lỗi", "Dữ liệu ghi phải là chuỗi Hex."); return
         # details = {'type': 'write_did', 'did': did_value, 'data': data_bytes}
         # # Need to find the correct parameter name for 'data' from ODX for the encode step
         # self._emit_diagnostic_action(details)


    # --- Methods to Update Results ---

    def clear_results(self):
        self.resultsTextEdit.clear()
        # Optionally keep raw log or clear it too
        # self.rawLogTextEdit.clear()

    def append_to_results(self, text):
        self.resultsTextEdit.append(text)
        QApplication.processEvents() # Force UI update for long operations

    def append_to_raw_log(self, text):
        self.rawLogTextEdit.append(text)
        QApplication.processEvents()

    def display_diagnostic_result(self, success, result_data, raw_request, raw_response):
        """Hiển thị kết quả từ DiagnosticWorker."""
        req_hex = raw_request.hex().upper() if raw_request else 'N/A'
        resp_hex = raw_response.hex().upper() if raw_response else 'N/A'
        self.append_to_raw_log(f"Request : {req_hex}")
        self.append_to_raw_log(f"Response: {resp_hex}")
        self.append_to_raw_log("-" * 30) # Separator

        if isinstance(result_data, str): # General error string from worker
             self.append_to_results(f"LỖI: {result_data}")
             return

        # --- Process structured result_data dictionary ---
        service_name = result_data.get('service_name', 'Unknown Service')
        response_type = result_data.get('response_type', 'Unknown')

        if success and response_type == 'Positive':
            self.append_to_results(f"THÀNH CÔNG: {service_name} (SID: 0x{raw_response[0]:02X})" if raw_response else service_name)
            params = result_data.get('parameters', {})
            if params:
                self.append_to_results("  Parameters:")
                for name, value in params.items():
                    # Special Formatting for DTCs (example parameter name: 'DTCAndStatusRecord')
                    # Check the actual parameter name in your ODX positive response definition!
                    dtc_param_name_heuristic = 'dtcandstatusrecord' # Lowercase for comparison
                    if name.lower() == dtc_param_name_heuristic and isinstance(value, list):
                        self.append_to_results(f"    {name}:")
                        if value:
                            # Assuming value is a list of tuples/objects: (dtc_int, status_byte)
                            for dtc_info in value:
                                try:
                                     # Adapt based on how odxtools returns decoded DTCs
                                     # Example: Assuming it returns a list of dicts or custom objects
                                     if isinstance(dtc_info, dict):
                                          dtc_val = dtc_info.get('DTCValue', '?') # Check ODX for actual key
                                          status_byte = dtc_info.get('StatusByte', 0x00) # Check ODX
                                     elif hasattr(dtc_info, 'DTCValue') and hasattr(dtc_info, 'StatusByte'): # Example custom object
                                         dtc_val = dtc_info.DTCValue
                                         status_byte = dtc_info.StatusByte
                                     else: # Fallback for simple tuple (dtc_int, status_byte)
                                         dtc_val, status_byte = dtc_info

                                     dtc_str = f"0x{dtc_val:06X}" if isinstance(dtc_val, int) else str(dtc_val)
                                     status_str = f"0x{status_byte:02X}"
                                     # TODO: Decode status byte bits if needed
                                     self.append_to_results(f"      - DTC: {dtc_str}, Status: {status_str}")
                                except Exception as fmt_e:
                                     self.append_to_results(f"      - Lỗi định dạng DTC: {dtc_info} ({fmt_e})")

                        else: # Empty list of DTCs
                            self.append_to_results("      (Không có DTC được báo cáo)")
                    else:
                        # Generic parameter formatting
                        value_str = repr(value)
                        if len(value_str) > 150: value_str = value_str[:150] + "..."
                        self.append_to_results(f"    {name}: {value_str}")
            else:
                 self.append_to_results("  (Phản hồi dương không có tham số dữ liệu)")

        elif response_type == 'Negative':
             nrc_info = result_data.get('nrc', {})
             nrc_code = nrc_info.get('code', 0xFF)
             nrc_desc = nrc_info.get('description', 'Unknown NRC')
             # Get the requested SID from the raw request if available
             req_sid_str = f"(Req SID: 0x{raw_request[0]:02X})" if raw_request else ""
             self.append_to_results(f"THẤT BẠI: {service_name} {req_sid_str}")
             self.append_to_results(f"  NRC: 0x{nrc_code:02X} - {nrc_desc}")
        else: # General failure reported by worker before full decoding
             self.append_to_results(f"LỖI THỰC THI: {service_name}")
             self.append_to_results(f"  Chi tiết: {result_data}") # result_data is the error string here


# --- MAIN APPLICATION WINDOW ---

class MultiCanManagerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.networks_data = {} # { network_id: { ... data ... }, ... }
        self.workers = {} # { worker_id (e.g., f"{net_id}_dbc"): worker_thread }
        self.next_network_id_counter = 1
        self.current_selected_network_id = None

        self.initUI()

    def initUI(self):
        self.setWindowTitle("Multi-Network CAN Manager & Diagnostics Tool")
        self.setGeometry(50, 50, 1500, 950) # Slightly larger default size

        # --- Menu ---
        self.setup_menu()
        # --- Toolbar (Optional, e.g., for connect/disconnect) ---
        self.setup_toolbar()

        # --- Bố cục chính với Splitter ---
        self.splitter = QSplitter(Qt.Horizontal)

        # --- Panel Trái: Cây Mạng ---
        self.networkTreeWidget = QTreeWidget()
        self.setup_network_tree()
        tree_container = QWidget()
        tree_layout = QVBoxLayout(tree_container)
        tree_layout.setContentsMargins(0,0,0,0)
        tree_layout.addWidget(self.networkTreeWidget)
        tree_container.setMaximumWidth(400) # Allow slightly wider tree
        self.splitter.addWidget(tree_container)


        # --- Panel Phải: Các Tab Chi tiết ---
        self.detailsTabWidget = QTabWidget()
        self.setup_detail_tabs() # Includes Diagnostics Tab now
        self.splitter.addWidget(self.detailsTabWidget)

        # Cấu hình Splitter
        self.splitter.setSizes([350, 1150]) # Adjust initial sizes
        self.splitter.setChildrenCollapsible(False)

        self.setCentralWidget(self.splitter)

        # --- Thanh trạng thái ---
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusLabel = QLabel("Sẵn sàng. Chọn mạng hoặc thêm mạng mới.")
        self.statusBar.addWidget(self.statusLabel, 1)

        # Add a default network on startup for convenience?
        # if not self.networks_data:
        #     self.add_new_network()

        self.show()

    def setup_menu(self):
        menu_bar = self.menuBar()
        # File Menu
        file_menu = menu_bar.addMenu("&File")
        exit_action = QAction(QIcon.fromTheme("application-exit"), "&Thoát", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Network Menu
        # --- GÁN VÀO self.network_menu NGAY KHI TẠO ---
        self.network_menu = menu_bar.addMenu("&Network")  # <<< GÁN NGAY TẠI ĐÂY

        # --- Bây giờ mới thêm các action vào self.network_menu đã tồn tại ---
        add_net_action = QAction(QIcon.fromTheme("network-wired"), "&Thêm Mạng CAN Mới", self)
        add_net_action.setShortcut("Ctrl+N")
        add_net_action.triggered.connect(self.add_new_network)
        self.network_menu.addAction(add_net_action)  # Bây giờ self.network_menu đã tồn tại

        self.network_menu.addSeparator()

        self.connect_action = QAction(QIcon.fromTheme("network-connect"), "Kết nối Mạng đã chọn", self)
        self.connect_action.triggered.connect(self.connect_selected_network)
        self.connect_action.setEnabled(False)
        self.network_menu.addAction(self.connect_action)

        self.disconnect_action = QAction(QIcon.fromTheme("network-disconnect"), "Ngắt kết nối Mạng đã chọn", self)
        self.disconnect_action.triggered.connect(self.disconnect_selected_network)
        self.disconnect_action.setEnabled(False)
        self.network_menu.addAction(self.disconnect_action)

        # Help Menu
        help_menu = menu_bar.addMenu("&Help")
        about_action = QAction(QIcon.fromTheme("help-about"), "&Giới thiệu", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)

    def setup_toolbar(self):
        """Sets up the main toolbar."""
        toolbar = QToolBar("Main Toolbar")
        toolbar.setIconSize(QSize(22, 22))
        self.addToolBar(toolbar)

        # SỬ DỤNG THAM CHIẾU TRỰC TIẾP self.network_menu
        if hasattr(self, 'network_menu') and self.network_menu:  # Kiểm tra xem network_menu đã được tạo chưa
            network_actions = self.network_menu.actions()
            if network_actions:  # Kiểm tra xem menu có action không
                toolbar.addAction(network_actions[0])  # Thêm action đầu tiên (Add New Network)
            toolbar.addSeparator()
            # Add connect/disconnect actions (chúng đã được lưu vào self.connect_action và self.disconnect_action)
            toolbar.addAction(self.connect_action)
            toolbar.addAction(self.disconnect_action)
        else:
            print("Cảnh báo: Không tìm thấy self.network_menu khi thiết lập toolbar.")

    def setup_network_tree(self):
        self.networkTreeWidget.setHeaderHidden(True)
        root_item = QTreeWidgetItem(self.networkTreeWidget, ["CAN Networks"])
        root_item.setIcon(0, QIcon.fromTheme("network-server")) # Icon for root
        self.networkTreeWidget.addTopLevelItem(root_item)
        root_item.setExpanded(True)
        root_item.setFlags(root_item.flags() & ~Qt.ItemIsSelectable & ~Qt.ItemIsEditable) # Root not selectable/editable

        self.networkTreeWidget.itemSelectionChanged.connect(self.on_network_selection_changed)
        self.networkTreeWidget.itemChanged.connect(self.on_network_item_changed)
        self.networkTreeWidget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.networkTreeWidget.customContextMenuRequested.connect(self.show_network_context_menu)

    def setup_detail_tabs(self):
        # Instantiate all tabs
        self.dbcTab = DbcConfigTab(self)
        self.traceTab = TraceMessagesTab(self)
        self.signalsTab = SignalDataTab(self)
        self.graphTab = GraphingTab(self)
        self.logTab = LoggingTab(self)
        self.diagTab = DiagnosticsTab(self) # Instantiate Diagnostics Tab

        # Add tabs to the QTabWidget
        self.detailsTabWidget.addTab(self.dbcTab, QIcon.fromTheme("database"), "Cấu hình DBC")
        self.detailsTabWidget.addTab(self.traceTab, QIcon.fromTheme("text-x-generic"), "Trace/Messages")
        self.detailsTabWidget.addTab(self.signalsTab, QIcon.fromTheme("view-list-details"), "Dữ liệu Tín hiệu")
        self.detailsTabWidget.addTab(self.graphTab, QIcon.fromTheme("utilities-system-monitor"), "Đồ thị")
        self.detailsTabWidget.addTab(self.logTab, QIcon.fromTheme("document-save"), "Thu Log")
        # Add the Diagnostics Tab
        self.detailsTabWidget.addTab(self.diagTab, QIcon.fromTheme("applications-engineering"), "Chẩn đoán") # Example icon

        # --- Connect Signals ---
        # DBC Tab
        self.dbcTab.loadDbcRequested.connect(self.handle_load_dbc)
        # Trace Tab
        self.traceTab.loadTraceRequested.connect(self.handle_load_trace)
        self.traceTab.signalValueUpdate.connect(self.handle_signal_value_update) # Connect to main handler
        # Log Tab
        self.logTab.selectLogFileRequested.connect(self.handle_select_log_file)
        self.logTab.toggleLoggingRequested.connect(self.handle_toggle_logging)
        # Diagnostics Tab
        self.diagTab.loadDiagFileRequested.connect(self.handle_load_diag_file)
        self.diagTab.diagnosticActionRequested.connect(self.handle_diagnostic_action)

        # Initially disable tabs until a network is selected
        self.detailsTabWidget.setEnabled(False)

    # --- Network Tree Context Menu ---
    def show_network_context_menu(self, position):
        selected_item = self.networkTreeWidget.currentItem()
        if not selected_item or selected_item.parent() is None: # Ensure it's a network item, not root
             return

        network_id = selected_item.data(0, Qt.UserRole)
        if not network_id or network_id not in self.networks_data:
            return

        menu = QMenu()
        is_connected = self.networks_data[network_id].get('is_connected', False)

        connect_act = menu.addAction(QIcon.fromTheme("network-connect"), "Kết nối")
        disconnect_act = menu.addAction(QIcon.fromTheme("network-disconnect"), "Ngắt kết nối")
        rename_act = menu.addAction(QIcon.fromTheme("edit-rename"), "Đổi tên")
        remove_act = menu.addAction(QIcon.fromTheme("edit-delete"), "Xóa mạng")

        connect_act.setEnabled(not is_connected)
        disconnect_act.setEnabled(is_connected)

        action = menu.exec_(self.networkTreeWidget.mapToGlobal(position))

        if action == connect_act:
            self.connect_network(network_id)
        elif action == disconnect_act:
            self.disconnect_network(network_id)
        elif action == rename_act:
            self.networkTreeWidget.editItem(selected_item, 0) # Start editing
        elif action == remove_act:
            self.remove_network(network_id, selected_item)

    # --- Network Management Methods ---
    def add_new_network(self):
        network_id = str(uuid.uuid4()) # Tạo ID duy nhất
        network_name = f"CAN Network {self.next_network_id_counter}"
        self.next_network_id_counter += 1

        # --- Initialize Network Data Structure ---
        self.networks_data[network_id] = {
            "name": network_name,
            # DBC/Trace/Log Data
            "dbc_path": None,
            "db": None, # cantools db object
            "trace_path": None,
            "trace_data": [],
            "signal_time_series": {},
            "latest_signal_values": {},
            "log_path": None,
            "is_logging": False,
            "logging_worker": None,
            # Diagnostics Data
            "diag_file_path": None,
            "odx_database": None, # odxtools database object
            "selected_ecu_diag_layer": None, # Selected odxtools DiagLayer object
            "diag_worker": None, # Reference to active diagnostic worker
            # Connection Data (Crucial!)
            "can_interface": "vector", # Default or get from settings
            "can_channel": "0",        # Default or get from settings
            "can_bitrate": 500000,     # Default or get from settings
            "can_bus": None,           # The python-can bus instance
            "is_connected": False,     # Connection status flag
            # Add other settings like FDCAN, filters if needed
        }

        # Add item to the network tree
        root = self.networkTreeWidget.topLevelItem(0)
        network_item = QTreeWidgetItem(root, [network_name])
        network_item.setData(0, Qt.UserRole, network_id) # Store unique ID
        network_item.setFlags(network_item.flags() | Qt.ItemIsEditable) # Allow renaming
        network_item.setIcon(0, QIcon.fromTheme("network-wired")) # Icon for network item
        root.addChild(network_item)
        root.setExpanded(True)

        # Automatically select the newly created network
        self.networkTreeWidget.setCurrentItem(network_item)
        self.statusLabel.setText(f"Đã thêm mạng mới: {network_name}")
        # Selection change will handle updating tabs and actions

    def remove_network(self, network_id, item):
        """Removes a network after confirmation."""
        if not network_id or network_id not in self.networks_data:
             return

        network_name = self.networks_data[network_id]['name']
        reply = QMessageBox.question(self, 'Xác nhận Xóa',
                                     f"Bạn có chắc chắn muốn xóa mạng '{network_name}' không?\n"
                                     f"Tác vụ này không thể hoàn tác.",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            # 1. Disconnect if connected
            if self.networks_data[network_id].get('is_connected', False):
                self.disconnect_network(network_id) # Ensure bus is shut down

            # 2. Stop any running workers for this network
            workers_to_remove = [wid for wid in self.workers if wid.startswith(network_id)]
            for worker_id in workers_to_remove:
                worker = self.workers[worker_id]
                if worker.isRunning():
                    try:
                        if hasattr(worker, 'stop'): worker.stop()
                        else: worker.quit()
                        worker.wait(300) # Wait briefly
                        if worker.isRunning(): worker.terminate() # Force if needed (last resort)
                    except Exception as e:
                        print(f"Lỗi khi dừng worker {worker_id} để xóa mạng: {e}")
                del self.workers[worker_id] # Remove from tracking dict

            # 3. Remove data
            del self.networks_data[network_id]

            # 4. Remove from tree
            (item.parent() or self.networkTreeWidget.invisibleRootItem()).removeChild(item)

            # 5. Update UI if it was the selected network
            if network_id == self.current_selected_network_id:
                self.current_selected_network_id = None
                self.detailsTabWidget.setEnabled(False)
                self.connect_action.setEnabled(False)
                self.disconnect_action.setEnabled(False)
                # Clear tabs content? Optional, or let selection change handle it.
                for i in range(self.detailsTabWidget.count()):
                     tab = self.detailsTabWidget.widget(i)
                     if hasattr(tab, 'update_content'):
                         tab.update_content(None, {}) # Pass empty data


            self.statusLabel.setText(f"Đã xóa mạng: {network_name}")


    def on_network_selection_changed(self):
        selected_items = self.networkTreeWidget.selectedItems()
        if not selected_items:
            network_id = None
        else:
            selected_item = selected_items[0]
            # Ensure a network item is selected, not the root
            if selected_item and selected_item.parent():
                network_id = selected_item.data(0, Qt.UserRole)
            else: # Root item selected or invalid item
                network_id = None

        # Update based on the determined network_id
        self.current_selected_network_id = network_id

        if network_id and network_id in self.networks_data:
            network_data = self.networks_data[network_id]
            network_name = network_data.get("name", "N/A")
            is_connected = network_data.get('is_connected', False)

            # Enable tabs and update their content
            self.detailsTabWidget.setEnabled(True)
            print(f"Selection changed to Network ID: {network_id}, Name: {network_name}, Connected: {is_connected}")
            for i in range(self.detailsTabWidget.count()):
                 tab_widget = self.detailsTabWidget.widget(i)
                 if hasattr(tab_widget, 'update_content') and callable(tab_widget.update_content):
                      tab_widget.update_content(network_id, network_data)

            # Update status bar
            status_text = f"Đang xem mạng: {network_name}"
            if is_connected:
                 bus_info = network_data.get('can_bus').channel_info if network_data.get('can_bus') else 'N/A'
                 status_text += f" (Đã kết nối: {bus_info})"
            else:
                 status_text += " (Chưa kết nối)"
            self.statusLabel.setText(status_text)

            # Update menu/toolbar actions
            self.connect_action.setEnabled(not is_connected)
            self.disconnect_action.setEnabled(is_connected)

        else:
            # No network selected or invalid ID
            self.detailsTabWidget.setEnabled(False)
            self.statusLabel.setText("Vui lòng chọn một mạng CAN từ danh sách.")
            self.connect_action.setEnabled(False)
            self.disconnect_action.setEnabled(False)
             # Optionally clear tab content when no network is selected
            for i in range(self.detailsTabWidget.count()):
                 tab = self.detailsTabWidget.widget(i)
                 if hasattr(tab, 'update_content'):
                     tab.update_content(None, {}) # Pass empty data

    def on_network_item_changed(self, item, column):
        """Handles renaming a network in the tree."""
        if column == 0: # Name column
            network_id = item.data(0, Qt.UserRole)
            if network_id and network_id in self.networks_data:
                new_name = item.text(0).strip()
                if not new_name: # Prevent empty names
                     item.setText(0, self.networks_data[network_id]['name']) # Revert
                     QMessageBox.warning(self, "Tên không hợp lệ", "Tên mạng không được để trống.")
                     return

                old_name = self.networks_data[network_id]['name']
                if new_name != old_name:
                    self.networks_data[network_id]['name'] = new_name
                    status_msg = f"Đã đổi tên '{old_name}' thành '{new_name}'"
                    if network_id == self.current_selected_network_id:
                        # Update status bar if the currently viewed network was renamed
                        is_connected = self.networks_data[network_id].get('is_connected', False)
                        conn_status = " (Đã kết nối)" if is_connected else " (Chưa kết nối)"
                        self.statusLabel.setText(f"Đang xem mạng: {new_name}{conn_status}")
                    else:
                        self.statusLabel.setText(status_msg)
                    print(status_msg)


    # --- Connection Handling ---
    def connect_selected_network(self):
         if self.current_selected_network_id:
              self.connect_network(self.current_selected_network_id)

    def disconnect_selected_network(self):
         if self.current_selected_network_id:
              self.disconnect_network(self.current_selected_network_id)

    def connect_network(self, network_id):
        """Connects the CAN bus for the specified network."""
        if not network_id or network_id not in self.networks_data:
            self.show_network_error(network_id, "Lỗi: ID mạng không hợp lệ.")
            return
        network_info = self.networks_data[network_id]

        if network_info.get('is_connected', False) and network_info.get('can_bus'):
            print(f"Mạng {network_info['name']} đã được kết nối.")
            QMessageBox.information(self, "Đã kết nối", f"Mạng '{network_info['name']}' đã được kết nối.")
            return

        # --- Get connection parameters (from network_info or a config dialog) ---
        interface = network_info.get('can_interface', 'vector') # Default to vector
        channel = network_info.get('can_channel', '0')
        bitrate = network_info.get('can_bitrate', 500000)
        # Add FD, filters, etc. as needed from network_info
        fd = False # Example: network_info.get('can_fd', False)
        # filters = network_info.get('can_filters', None) # Example: [{"can_id": 0x123, "can_mask": 0x7FF}]

        try:
            self.statusLabel.setText(f"Net {network_info['name']}: Đang kết nối {interface} channel {channel} @ {bitrate} bps...")
            QApplication.processEvents() # Update UI immediately

            bus_args = {
                'interface': interface,
                'channel': channel,
                'bitrate': bitrate,
                'fd': fd,
                # 'can_filters': filters
            }
            # Add interface-specific arguments
            if interface == 'vector':
                bus_args['app_name'] = 'MultiCANApp' # Recommended for Vector

            print(f"Attempting connection with args: {bus_args}")
            bus = can.interface.Bus(**bus_args)

            network_info['can_bus'] = bus
            network_info['is_connected'] = True
            print(f"Net {network_info['name']}: Connected successfully via {bus.channel_info}")
            self.statusLabel.setText(f"Net {network_info['name']}: Đã kết nối ({bus.channel_info})")

            # Update UI state (tree icon, actions, tabs)
            self.update_network_item_icon(network_id, True)
            if network_id == self.current_selected_network_id:
                self.on_network_selection_changed() # Refresh tabs and actions

        except Exception as e:
             network_info['can_bus'] = None
             network_info['is_connected'] = False
             self.show_network_error(network_id, f"Lỗi kết nối CAN:\n{e}\n\nArgs: {bus_args}\n\n{traceback.format_exc()}")
             # Update UI state
             self.update_network_item_icon(network_id, False)
             if network_id == self.current_selected_network_id:
                self.on_network_selection_changed() # Refresh actions

    def disconnect_network(self, network_id):
        """Disconnects the CAN bus for the specified network."""
        if not network_id or network_id not in self.networks_data: return
        network_info = self.networks_data[network_id]

        if not network_info.get('is_connected', False) or not network_info.get('can_bus'):
            print(f"Mạng {network_info['name']} chưa được kết nối hoặc bus không tồn tại.")
            # Ensure state is consistent
            network_info['is_connected'] = False
            network_info['can_bus'] = None
            self.update_network_item_icon(network_id, False)
            if network_id == self.current_selected_network_id: self.on_network_selection_changed()
            return

        try:
            self.statusLabel.setText(f"Net {network_info['name']}: Đang ngắt kết nối...")
            QApplication.processEvents()

            bus = network_info['can_bus']
            if bus:
                bus.shutdown() # Crucial step!
                print(f"Net {network_info['name']}: Bus shutdown() called.")

            network_info['can_bus'] = None
            network_info['is_connected'] = False
            self.statusLabel.setText(f"Net {network_info['name']}: Đã ngắt kết nối.")

        except Exception as e:
             # Even if shutdown fails, force the state update
             network_info['can_bus'] = None
             network_info['is_connected'] = False
             self.show_network_error(network_id, f"Lỗi ngắt kết nối CAN:\n{e}\n{traceback.format_exc()}")

        finally:
            # Update UI state regardless of errors during shutdown
            self.update_network_item_icon(network_id, False)
            if network_id == self.current_selected_network_id:
                self.on_network_selection_changed() # Refresh tabs and actions

    def update_network_item_icon(self, network_id, is_connected):
        """Updates the icon in the network tree based on connection status."""
        root = self.networkTreeWidget.topLevelItem(0)
        for i in range(root.childCount()):
            item = root.child(i)
            if item.data(0, Qt.UserRole) == network_id:
                 if is_connected:
                      item.setIcon(0, QIcon.fromTheme("network-transmit-receive")) # Connected icon
                 else:
                      item.setIcon(0, QIcon.fromTheme("network-wired")) # Disconnected icon
                 break

    # --- File/Action Handlers ---

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
            self.update_network_status(network_id, f"Bắt đầu tải DBC: {os.path.basename(file_path)}...")
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

        file_path, _ = QFileDialog.getOpenFileName(self, f"Chọn File Trace CSV cho Mạng {self.networks_data[network_id]['name']}", dir_path, "CSV Files (*.csv);;Log Files (*.log);;All Files (*)")
        if file_path:
            self.update_network_status(network_id, f"Bắt đầu tải Trace: {os.path.basename(file_path)}...")
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
             if not file_path.lower().endswith(".csv"):
                 file_path += ".csv"
             self.networks_data[network_id]['log_path'] = file_path
             self.update_network_status(network_id, f"Đã chọn file log: {os.path.basename(file_path)}")
             # Update log tab if current
             if network_id == self.current_selected_network_id:
                 self.logTab.update_content(network_id, self.networks_data[network_id])

    def handle_toggle_logging(self, network_id):
        if not network_id or network_id not in self.networks_data: return
        network_info = self.networks_data[network_id]
        is_currently_logging = network_info.get('is_logging', False)
        log_path = network_info.get('log_path', None)
        worker_id = f"{network_id}_logging"

        if not is_currently_logging: # --- Start Logging ---
            if not log_path:
                 QMessageBox.warning(self, "Thiếu File Log", f"Vui lòng chọn file log cho mạng {network_info['name']} trước.")
                 if network_id == self.current_selected_network_id: self.logTab.update_content(network_id, network_info) # Update button state
                 return

            if worker_id in self.workers and self.workers[worker_id].isRunning():
                 print(f"Cảnh báo: Worker log {worker_id} dường như đang chạy?")
                 # Attempt to stop cleanly before starting new one
                 self.workers[worker_id].stop()
                 if not self.workers[worker_id].wait(500):
                      print(f"Warning: Could not stop previous log worker {worker_id} cleanly.")
                      self.workers[worker_id].terminate() # Force stop if necessary
                 del self.workers[worker_id]


            log_worker = LoggingWorker(network_id, log_path)
            log_worker.status.connect(self.update_network_status)
            log_worker.error.connect(self.show_network_error)

            # --- TODO: Connect actual CAN message source to log_worker.add_message_to_queue ---
            # This requires modifying the CAN reading part (if live) or feeding from trace
            # Example: If reading live CAN, the reader thread should call:
            # if network_info['is_logging'] and network_info['logging_worker']:
            #    network_info['logging_worker'].add_message_to_queue([timestamp, id_hex, dlc, data_hex])
            # For now, just log existing trace data when starting (not live)
            if network_info.get('trace_data'):
                 count = 0
                 for msg_row in network_info['trace_data']:
                      log_worker.add_message_to_queue(msg_row[:4]) # Ensure only needed columns
                      count += 1
                 self.update_network_status(network_id, f"Đã thêm {count} tin nhắn từ trace vào hàng đợi log.")


            network_info['is_logging'] = True
            network_info['logging_worker'] = log_worker
            self.workers[worker_id] = log_worker
            log_worker.start()
            self.update_network_status(network_id, f"Bắt đầu ghi log...")

        else: # --- Stop Logging ---
            log_worker = network_info.get('logging_worker')
            if log_worker and log_worker.isRunning():
                self.update_network_status(network_id, f"Đang dừng ghi log...")
                log_worker.stop() # Request worker stop (it handles writing remaining queue)
                # Don't delete worker here, let it finish and clean up in its finished signal if needed
            else:
                 print(f"Cảnh báo: Không tìm thấy worker log đang chạy cho {network_id} để dừng.")
                 network_info['is_logging'] = False # Force state update

            network_info['is_logging'] = False
            # Keep worker ref until it confirms stopped? Or clear immediately?
            # network_info['logging_worker'] = None # Clear ref
            # if worker_id in self.workers: del self.workers[worker_id] # Remove tracking?

        # Update log tab UI
        if network_id == self.current_selected_network_id:
            self.logTab.update_content(network_id, network_info)


    def handle_load_diag_file(self, network_id):
        """Handles request to load PDX/ODX file."""
        if not network_id or network_id not in self.networks_data: return
        worker_id = f"{network_id}_diagload"
        if worker_id in self.workers and self.workers[worker_id].isRunning():
             QMessageBox.information(self, "Đang xử lý", f"Đang tải file chẩn đoán cho mạng {self.networks_data[network_id]['name']}...")
             return

        current_path = self.networks_data[network_id].get('diag_file_path', None)
        dir_path = os.path.dirname(current_path) if current_path else ""

        file_path, _ = QFileDialog.getOpenFileName(
            self, f"Chọn File PDX/ODX cho Mạng {self.networks_data[network_id]['name']}",
            dir_path, "Diagnostic Files (*.pdx *.odx *.odx-d);;All Files (*)" # CDD not supported by odxtools
        )
        if file_path:
            self.update_network_status(network_id, f"Bắt đầu tải file chẩn đoán: {os.path.basename(file_path)}...")
            worker = DiagFileLoadingWorker(network_id, file_path)
            worker.finished.connect(self.on_diag_file_loaded)
            self.workers[worker_id] = worker
            worker.start()

    def handle_diagnostic_action(self, network_id, request_details):
        """Handles request from DiagnosticsTab to perform an action."""
        if not network_id or network_id not in self.networks_data:
            self.show_network_error(network_id, "Lỗi: Network ID không hợp lệ.")
            return

        worker_id = f"{network_id}_diagaction"
        if worker_id in self.workers and self.workers[worker_id].isRunning():
             QMessageBox.information(self, "Đang xử lý", f"Đang thực hiện tác vụ chẩn đoán khác cho mạng {self.networks_data[network_id]['name']}...")
             return

        network_info = self.networks_data[network_id]

        # --- CRITICAL CHECKS ---
        if not network_info.get('is_connected', False) or network_info.get('can_bus') is None:
             QMessageBox.critical(self, "Lỗi Kết Nối", f"Mạng '{network_info['name']}' chưa được kết nối. Vui lòng kết nối phần cứng trước.")
             return
        if not network_info.get('selected_ecu_diag_layer'):
             QMessageBox.critical(self, "Lỗi Cấu hình", f"Chưa chọn ECU/Variant hợp lệ cho mạng '{network_info['name']}'.")
             return
        if not network_info.get('odx_database'):
             QMessageBox.critical(self, "Lỗi Cấu hình", f"Chưa tải file ODX/PDX hợp lệ cho mạng '{network_info['name']}'.")
             return

        self.update_network_status(network_id, f"Bắt đầu tác vụ chẩn đoán: {request_details.get('type', 'N/A')}...")
        # Disable relevant buttons in DiagTab?
        if network_id == self.current_selected_network_id:
             self.diagTab.set_service_controls_enabled(False) # Disable UI during execution

        worker = DiagnosticWorker(network_id, self, request_details) # Pass self reference
        worker.finished.connect(self.on_diagnostic_action_finished)
        worker.progress.connect(self.update_network_status)
        network_info['diag_worker'] = worker
        self.workers[worker_id] = worker
        worker.start()


    # --- Worker Finished Slots ---

    def on_dbc_loaded(self, network_id, db_or_none, path_or_error):
        worker_id = f"{network_id}_dbc"
        if worker_id in self.workers: del self.workers[worker_id]

        if network_id not in self.networks_data: return

        network_info = self.networks_data[network_id]
        if db_or_none is not None:
            network_info['db'] = db_or_none
            network_info['dbc_path'] = path_or_error
            self.update_network_status(network_id, f"Đã tải DBC thành công: {os.path.basename(path_or_error)}")
            # Clear derived data
            network_info['latest_signal_values'] = {}
            network_info['signal_time_series'] = {}
             # Update relevant tabs if current
            if network_id == self.current_selected_network_id:
                self.dbcTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info)
                self.graphTab.update_content(network_id, network_info)
                # Re-populate trace tab with new decoding (or prompt user)
                if network_info.get('trace_data'):
                     self.traceTab.populate_trace_table(network_info['trace_data'], db_or_none)
                     self.update_network_status(network_id, "Đã làm mới bảng trace với DBC mới.")

        else:
            network_info['db'] = None
            network_info['dbc_path'] = None
            network_info['latest_signal_values'] = {}
            network_info['signal_time_series'] = {}
            self.show_network_error(network_id, f"Lỗi tải DBC:\n{path_or_error}")
            if network_id == self.current_selected_network_id:
                self.dbcTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info)
                self.graphTab.update_content(network_id, network_info)
                if network_info.get('trace_data'): # Show trace without decoding
                     self.traceTab.populate_trace_table(network_info['trace_data'], None)

    def on_trace_loaded(self, network_id, trace_data_or_none, signal_timeseries, path_or_error):
        worker_id = f"{network_id}_trace"
        if worker_id in self.workers: del self.workers[worker_id]

        if network_id not in self.networks_data: return

        network_info = self.networks_data[network_id]
        if trace_data_or_none is not None:
            network_info['trace_data'] = trace_data_or_none
            network_info['trace_path'] = path_or_error
            network_info['signal_time_series'] = signal_timeseries
            # Recalculate latest values
            network_info['latest_signal_values'] = self._get_latest_values_from_timeseries(signal_timeseries)

            self.update_network_status(network_id, f"Đã tải Trace ({len(trace_data_or_none)} msgs): {os.path.basename(path_or_error)}")

            # Update tabs if current
            if network_id == self.current_selected_network_id:
                self.traceTab.update_content(network_id, network_info)
                self.signalsTab.update_content(network_id, network_info)
                self.graphTab.update_content(network_id, network_info)

            # If logging, add new trace data to queue? (Decide on behavior)
            # if network_info.get('is_logging') and network_info.get('logging_worker'):
            #      lw = network_info['logging_worker']
            #      count = 0
            #      for msg_row in trace_data_or_none:
            #           lw.add_message_to_queue(msg_row[:4])
            #           count += 1
            #      self.update_network_status(network_id, f"Đã thêm {count} tin nhắn từ trace mới vào log.")

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
        """Helper to get the last value for each signal from timeseries data."""
        latest_values = {}
        for sig_name, (timestamps, values) in signal_timeseries.items():
             if timestamps and values:
                  # Assuming timestamps are sorted, the last element is the latest
                  latest_values[sig_name] = (values[-1], timestamps[-1])
        return latest_values

    def on_diag_file_loaded(self, network_id, odx_database_or_none, path_or_error):
        """Handles result from DiagFileLoadingWorker."""
        worker_id = f"{network_id}_diagload"
        if worker_id in self.workers: del self.workers[worker_id]

        if network_id not in self.networks_data: return

        network_info = self.networks_data[network_id]
        if odx_database_or_none is not None and isinstance(odx_database_or_none, odxtools.Database):
            network_info['odx_database'] = odx_database_or_none
            network_info['diag_file_path'] = path_or_error
            network_info['selected_ecu_diag_layer'] = None # Reset ECU selection
            self.update_network_status(network_id, f"Đã tải file chẩn đoán: {os.path.basename(path_or_error)}")
            if network_id == self.current_selected_network_id:
                self.diagTab.update_content(network_id, network_info) # Refresh DiagTab
        else:
            network_info['odx_database'] = None
            network_info['diag_file_path'] = None
            network_info['selected_ecu_diag_layer'] = None
            self.show_network_error(network_id, f"Lỗi tải file chẩn đoán:\n{path_or_error}")
            if network_id == self.current_selected_network_id:
                self.diagTab.update_content(network_id, network_info) # Refresh DiagTab


    def on_diagnostic_action_finished(self, network_id, success, result_data, raw_request, raw_response):
        """Handles result from DiagnosticWorker."""
        worker_id = f"{network_id}_diagaction"
        if worker_id in self.workers: del self.workers[worker_id]

        if network_id not in self.networks_data: return

        network_info = self.networks_data[network_id]
        network_info['diag_worker'] = None # Clear worker reference

        # Generate status message
        status_msg = f"Net {network_info['name']}: Tác vụ chẩn đoán "
        if isinstance(result_data, str): # General error string
            status_msg += f"thất bại/lỗi."
        elif isinstance(result_data, dict):
             resp_type = result_data.get('response_type', 'Unknown')
             if resp_type == 'Positive': status_msg += "hoàn thành."
             elif resp_type == 'Negative':
                  nrc_code = result_data.get('nrc', {}).get('code', 0xFF)
                  status_msg += f"thất bại (NRC: 0x{nrc_code:02X})."
             else: status_msg += "kết thúc với trạng thái không xác định."
        else: status_msg += "kết thúc."

        self.update_network_status(network_id, status_msg, is_long_status=False) # Update status bar

        # Update the Diagnostics tab UI if it's the current network
        if network_id == self.current_selected_network_id:
             self.diagTab.display_diagnostic_result(success, result_data, raw_request, raw_response)
             self.diagTab.set_service_controls_enabled(True) # Re-enable UI controls


    # --- Signal Update Handler ---
    def handle_signal_value_update(self, network_id, signal_name, value, timestamp_obj):
         """Handles signal value updates decoded from TraceMessagesTab."""
         if network_id in self.networks_data:
              # Update the central latest value store
              self.networks_data[network_id]['latest_signal_values'][signal_name] = (value, timestamp_obj)

              # If this network is currently selected, update the SignalDataTab
              if network_id == self.current_selected_network_id:
                   self.signalsTab.update_signal_value(signal_name, value, timestamp_obj)
                   # TODO: Optional - Add live update to GraphingTab if feasible
                   # self.graphTab.append_data_point(signal_name, timestamp_obj, value)


    # --- Status and Error Updates ---

    def update_network_status(self, network_id, message, is_long_status=True):
        """Updates the main status bar, prefixing with network name if ID is known."""
        prefix = ""
        if network_id and network_id in self.networks_data:
             prefix = f"Net {self.networks_data[network_id]['name']}: "

        full_message = f"{prefix}{message}"
        # Truncate long messages for status bar if needed
        if is_long_status and len(full_message) > 150:
             full_message = full_message[:147] + "..."

        self.statusLabel.setText(full_message)
        print(f"Status Update ({network_id if network_id else 'Global'}): {message}") # Also print to console

    def update_network_progress_percent(self, network_id, percent):
         """Updates status bar with percentage."""
         prefix = ""
         if network_id and network_id in self.networks_data:
             prefix = f"Net {self.networks_data[network_id]['name']}: "
         self.statusLabel.setText(f"{prefix}Đang xử lý... {percent}%")


    def show_network_error(self, network_id, error_message):
        """Displays an error message in a dialog box, identifying the network."""
        net_name = "Chung"
        if network_id and network_id in self.networks_data:
            net_name = self.networks_data[network_id]['name']
        else: # Try to extract from worker ID if passed that way
             try: network_id_part = network_id.split('_')[0]
             except: network_id_part = network_id
             net_name = self.networks_data.get(network_id_part, {}).get('name', f"ID {network_id_part}")


        error_title = f"Lỗi - Mạng {net_name}"
        print(f"ERROR ({net_name}): {error_message}") # Log to console too
        self.statusLabel.setText(f"Net {net_name}: Có lỗi xảy ra (xem chi tiết).")
        # Show details in the message box
        QMessageBox.critical(self, error_title, str(error_message))


    # --- Utility Methods ---
    def show_about_dialog(self):
        QMessageBox.about(self, "Giới thiệu",
                          "<b>Multi-Network CAN Manager & Diagnostics Tool</b><br>"
                          "Phiên bản 0.2 (Tích hợp Chẩn đoán Cơ bản)<br><br>"
                          "Sử dụng PyQt5, python-can, cantools, odxtools, isotp, pyqtgraph.<br>"
                          "Lưu ý: Chức năng chẩn đoán yêu cầu file ODX/PDX chính xác và kết nối phần cứng phù hợp.<br><br>"
                          "(c) 2024")

    def closeEvent(self, event):
        """Handles application closing, stops workers and disconnects buses."""
        print("Close event triggered...")

        # --- Confirmation Dialog ---
        running_tasks = []
        # Check running workers
        for wid, w in self.workers.items():
             if w.isRunning():
                  net_id_part = wid.split('_')[0]
                  net_name = self.networks_data.get(net_id_part, {}).get('name', net_id_part)
                  task_type = wid.split('_')[-1]
                  running_tasks.append(f"Worker {task_type} (Mạng: {net_name})")
        # Check active connections
        active_connections = [data['name'] for nid, data in self.networks_data.items() if data.get('is_connected')]
        if active_connections:
             running_tasks.append(f"Kết nối CAN đang hoạt động ({', '.join(active_connections)})")

        if running_tasks:
             reply = QMessageBox.question(self, 'Xác nhận Thoát',
                                          "Các tác vụ hoặc kết nối sau đang hoạt động:\n- " + \
                                          "\n- ".join(running_tasks) + \
                                          "\n\nThoát ứng dụng sẽ cố gắng dừng chúng. Tiếp tục?",
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
             if reply == QMessageBox.No:
                  event.ignore()
                  print("Thoát bị hủy bởi người dùng.")
                  return
        else: # No running tasks, proceed directly
             event.accept()
             print("Không có tác vụ chạy nền hoặc kết nối hoạt động. Đang thoát...")
             return # Exit early


        # --- Shutdown Process (if user confirmed Yes) ---
        self.statusLabel.setText("Đang đóng ứng dụng...")
        QApplication.processEvents()

        # 1. Stop all running workers
        print("Yêu cầu các worker dừng...")
        workers_to_stop = list(self.workers.keys()) # Copy keys as dict might change
        for worker_id in workers_to_stop:
             if worker_id in self.workers and self.workers[worker_id].isRunning():
                  worker = self.workers[worker_id]
                  print(f"  - Dừng worker: {worker_id}")
                  try:
                       if hasattr(worker, 'stop'): worker.stop()
                       else: worker.quit() # Request event loop exit

                       if not worker.wait(500): # Wait max 500ms
                            print(f"  - Cảnh báo: Worker {worker_id} không dừng kịp thời, có thể cần terminate.")
                            # worker.terminate() # Use terminate as last resort - can cause issues
                  except Exception as e:
                       print(f"  - Lỗi khi dừng worker {worker_id}: {e}")
        print("Đã yêu cầu dừng tất cả worker.")
        QApplication.processEvents()


        # 2. Disconnect all active CAN buses
        print("Ngắt kết nối các mạng CAN...")
        nets_to_disconnect = [nid for nid, data in self.networks_data.items() if data.get('is_connected')]
        for net_id in nets_to_disconnect:
            print(f"  - Ngắt kết nối mạng: {self.networks_data[net_id]['name']}")
            self.disconnect_network(net_id) # This already updates UI potentially
            QApplication.processEvents() # Allow disconnect events
        print("Đã ngắt kết nối tất cả mạng CAN.")

        # 3. Accept the close event
        print("Đóng ứng dụng hoàn tất.")
        event.accept()


# --- Main Execution ---
if __name__ == '__main__':
    # Setup for high-DPI displays (optional but recommended)
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)
    # Apply a style (optional)
    # app.setStyle('Fusion')

    manager = MultiCanManagerApp()
    manager.show()
    sys.exit(app.exec_())
