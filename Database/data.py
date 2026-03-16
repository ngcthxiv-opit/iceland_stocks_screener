import re
import numpy as np
import pandas as pd
import warnings
import os

# Tắt các cảnh báo không cần thiết của Pandas
pd.set_option('future.no_silent_downcasting', True)
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# ==============================================================
# PHẦN 1: TỪ ĐIỂN VÀ CÁC BIẾN HẰNG SỐ (CONSTANTS)
# ==============================================================
VI_MAP = {
    "Company Common Name": "Tên công ty",
    "Country of Exchange": "Sàn giao dịch",
    "GICS Industry Name": "Ngành GICS",
    "GICS Sector Name": "Lĩnh vực GICS",
    "Date Became Public": "Ngày niêm yết",
    "Price Close": "Giá đóng cửa",
    "Price Open": "Giá mở cửa",
    "Price High": "Giá cao nhất",
    "Price Low": "Giá thấp nhất",
    "Volume": "Khối lượng giao dịch",

    # Balance sheet (BS)
    "Total Assets": "Tổng tài sản",
    "Total Current Assets": "Tổng tài sản ngắn hạn",
    "Total Non Current Assets": "Tổng tài sản dài hạn",
    "Total Liabilities": "Tổng nợ phải trả",
    "Total Current Liabilities": "Tổng nợ ngắn hạn",
    "Total Non Current Liabilities": "Tổng nợ dài hạn",
    "Total Equity": "Tổng vốn chủ sở hữu",
    "Cash & Cash Equivalents - Total": "Tiền và tương đương tiền",
    "Cash & Short Term Investments": "Tiền và đầu tư ngắn hạn",
    "Short-Term Investments - Total": "Đầu tư ngắn hạn",
    "Loans & Receivables - Net - Short-Term": "Khoản vay và phải thu ngắn hạn",
    "Trade Accounts & Trade Notes Receivable - Net": "Phải thu thương mại",
    "Loans - Short-Term": "Khoản vay ngắn hạn",
    "Income Tax - Receivables - Short-Term": "Thuế thu nhập phải thu ngắn hạn",
    "Receivables - Other - Total": "Phải thu khác",
    "Provision - Doubtful Accounts": "Dự phòng nợ khó đòi",
    "Inventories - Total": "Tổng hàng tồn kho",
    "Inventories - Raw Materials": "Hàng tồn kho - Nguyên vật liệu",
    "Inventories - Work in Progress": "Hàng tồn kho - Sản phẩm dở dang",
    "Inventories - Finished Goods": "Hàng tồn kho - Thành phẩm",
    "Inventories - Other - Total": "Hàng tồn kho - Khác",
    "Prepaid Expenses - Short-Term": "Chi phí trả trước ngắn hạn",
    "Other Current Assets - Total": "Tài sản ngắn hạn khác",
    "Investments - Long-Term": "Đầu tư dài hạn",
    "Investments - Permanent": "Đầu tư lâu dài",
    "Investments - Total": "Tổng đầu tư",
    "Loans & Receivables - Total": "Tổng khoản vay và phải thu",
    "Property Plant & Equipment - Net - Total": "Tài sản cố định ròng",
    "Property Plant & Equipment - Gross - Total": "Tài sản cố định gốc",
    "PPE - Accumulated Depreciation & Impairment - Total": "TSCĐ - Khấu hao lũy kế",
    "Land & Buildings - Accumulated Depreciation & Impairment": "Đất và nhà - Khấu hao lũy kế",
    "Buildings - Accumulated Depreciation & Impairment": "Nhà xưởng - Khấu hao lũy kế",
    "Plant, Machinery & Equipment - Accum Depreciation & Impair": "Máy móc thiết bị - Khấu hao lũy kế",
    "Transportation Equipment - Accumulated Depreciation & Impair": "Phương tiện vận tải - Khấu hao lũy kế",
    "Property, Plant & Equipment - Other - Accum Depr & Impair": "TSCĐ khác - Khấu hao lũy kế",
    "Investment Property - Accumulated Depreciation & Impairment": "Bất động sản đầu tư - Khấu hao lũy kế",
    "Natural Resources/Biological Assets - Accum Depr & Impair": "Tài nguyên thiên nhiên - Khấu hao lũy kế",
    "PPE - excl Assets Leased Out - Accum Depr & Impair - Total": "TSCĐ không bao gồm cho thuê - Khấu hao lũy kế",
    "Property Plant & Equipment - Other - Net": "TSCĐ khác - Ròng",
    "Other Assets - Total": "Tài sản khác",
    "Total Fixed Assets - Net": "Tổng tài sản cố định ròng",
    "Working Capital": "Vốn luân chuyển",
    "Working Capital excluding Other Current Assets & Liabilities": "Vốn luân chuyển không bao gồm TS/N khác",
    "Book Value excluding Other Equity": "Giá trị sổ sách không bao gồm vốn khác",
    "Net Book Capital": "Vốn sổ sách ròng",
    "Net Operating Assets": "Tài sản hoạt động ròng",
    "Net Debt": "Nợ ròng",
    "Tangible Total Equity": "Tổng vốn hữu hình",
    "Total Capital": "Tổng vốn",
    "Total Long Term Capital": "Tổng vốn dài hạn",
    "Inventory - Total": "Hàng tồn kho",
    "Accounts Receivable - Total": "Phải thu khách hàng",
    "Property Plant & Equipment - Total": "TSCĐ hữu hình",
    "Goodwill": "Lợi thế thương mại",
    "Intangible Assets - Total": "Tài sản vô hình",
    "Short Term Debt": "Nợ vay ngắn hạn",
    "Long Term Debt": "Nợ vay dài hạn",
    "Short-Term Debt & Notes Payable": "Nợ và phải trả ngắn hạn",
    "Short-Term Debt & Current Portion of Long-Term Debt": "Nợ ngắn hạn và phần ngắn hạn của nợ dài hạn",
    "Current Portion of Long-Term Debt incl Capitalized Leases": "Phần ngắn hạn của nợ dài hạn",
    "Current Portion of Long-Term Debt excl Capitalized Leases": "Phần ngắn hạn của nợ dài hạn không bao gồm thuê TT",
    "Debt - Long-Term - Total": "Tổng nợ dài hạn",
    "Debt - Non-Convertible - Long-Term": "Nợ không chuyển đổi dài hạn",
    "Debt - Total": "Tổng nợ",
    "Total Non-Current Liabilities": "Tổng nợ dài hạn",
    "Total Current Liabilities": "Tổng nợ ngắn hạn",
    "Other Current Liabilities": "Nợ ngắn hạn khác",
    "Other Current Liabilities - Total": "Tổng nợ ngắn hạn khác",
    "Other Non-Current Liabilities - Total": "Tổng nợ dài hạn khác",
    "Trade Accounts Payable & Accruals - Short-Term": "Phải trả thương mại và trích trước ngắn hạn",
    "Trade Accounts & Trade Notes Payable - Short-Term": "Phải trả thương mại ngắn hạn",
    "Trade Account Payables - Total": "Tổng phải trả thương mại",
    "Trade Accounts Payable - Long-Term": "Phải trả thương mại dài hạn",
    "Payables & Accrued Expenses": "Phải trả và chi phí trích trước",
    "Accounts Payable including Accrued Expenses - Long-Term": "Phải trả bao gồm chi phí trích trước dài hạn",
    "Accrued Expenses": "Chi phí trích trước",
    "Accrued Expenses - Short-Term": "Chi phí trích trước ngắn hạn",
    "Accrued Expenses - Long-Term": "Chi phí trích trước dài hạn",
    "Provisions - Short-Term": "Dự phòng ngắn hạn",
    "Provisions - Long-Term": "Dự phòng dài hạn",
    "Deferred Income - Short-Term": "Thu nhập hoãn lại ngắn hạn",
    "Deferred Revenue - Long-Term": "Doanh thu hoãn lại dài hạn",
    "Deferred Tax - Liability - Long-Term": "Thuế hoãn lại dài hạn",
    "Deferred Tax & Investment Tax Credits - Long-Term": "Thuế hoãn lại và ưu đãi thuế đầu tư dài hạn",
    "Customer Advances - Short-Term": "Ứng trước của khách hàng ngắn hạn",
    "Income Taxes - Payable - Short-Term": "Thuế thu nhập phải trả ngắn hạn",
    "Income Taxes - Payable - Long-Term & Short-Term": "Thuế thu nhập phải trả",
    "Provision - Doubtful Accounts - Total": "Tổng dự phòng nợ khó đòi",
    "Accounts & Notes Receivable - Trade - Gross - Total": "Tổng phải thu thương mại gộp",
    
    # Equity
    "Common Equity - Total": "Tổng vốn cổ phiếu thường",
    "Common Equity Attributable to Parent Shareholders": "Vốn cổ phiếu thường thuộc cổ đông công ty mẹ",
    "Shareholders Equity - Common": "Vốn cổ đông thường",
    "Shareholders' Equity - Attributable to Parent ShHold - Total": "Tổng vốn cổ đông thuộc công ty mẹ",
    "Total Shareholders' Equity incl Minority Intr & Hybrid Debt": "Tổng vốn cổ đông bao gồm cổ đông thiểu số",
    "Minority Interest": "Lợi ích cổ đông thiểu số",
    "Retained Earnings - Total": "Tổng lợi nhuận giữ lại",
    "Comprehensive Income - Accumulated - Total": "Tổng thu nhập toàn diện lũy kế",
    "Common Shares - Issued - Total": "Tổng cổ phiếu thường đã phát hành",
    "Common Shares - Outstanding - Total": "Tổng cổ phiếu thường đang lưu hành",
    "Total Liabilities & Equity": "Tổng nợ và vốn chủ sở hữu",

    # Income statement (IS)
    "Total Revenue": "Tổng doanh thu",
    "Revenue": "Doanh thu",
    "Revenue from Business Activities - Total": "Doanh thu từ hoạt động kinh doanh",
    "Sales of Goods & Services - Net - Unclassified": "Doanh thu bán hàng và dịch vụ",
    "Sales Returns Allowances & Other Revenue Adjustments": "Giảm trừ doanh thu",
    "Gross Revenue from Business Activities - Total": "Tổng doanh thu gộp từ hoạt động kinh doanh",
    "Revenue from Business-Related Activities - Other - Total": "Doanh thu khác liên quan kinh doanh",
    "Revenue from Goods & Services": "Doanh thu từ hàng hóa và dịch vụ",
    "Cost of Operating Revenue": "Giá vốn doanh thu hoạt động",
    "Cost of Revenues - Total": "Tổng giá vốn",
    "Cost of Revenue, Total": "Giá vốn hàng bán",
    "Gross Profit": "Lợi nhuận gộp",
    "Gross Profit - Industrials/Property - Total": "Tổng lợi nhuận gộp",
    "Operating Income": "Lợi nhuận hoạt động",
    "Operating Income After Depreciation": "LN hoạt động sau khấu hao",
    "Operating Expenses - Total": "Tổng chi phí hoạt động",
    "Operating Lease Payments - Total": "Tổng thanh toán thuê hoạt động",
    "Selling/General/Admin. Expenses, Total": "Chi phí bán hàng & QLDN",
    "Property & Other Taxes": "Thuế tài sản và khác",
    "Depreciation - Total": "Tổng khấu hao",
    "Depreciation Depletion & Amortization - Total": "Tổng khấu hao, hao mòn và phân bổ",
    "Depreciation Depletion & Amortization - Cash Flow": "Khấu hao, hao mòn và phân bổ - LCTT",
    "Depreciation & Amortization - Supplemental": "Khấu hao và phân bổ - Bổ sung",
    "Depreciation & Depletion - PPE - CF - to Reconcile": "Khấu hao TSCĐ - LCTT",
    "Amortization of Goodwill - Total": "Phân bổ lợi thế thương mại",
    "Amortization of Intangible Assets excluding Goodwill - Total": "Phân bổ tài sản vô hình",
    "Net Income": "Lợi nhuận sau thuế",
    "Net Income after Tax": "Lợi nhuận sau thuế",
    "Net Income after Minority Interest": "Lợi nhuận sau lợi ích thiểu số",
    "Net Income before Minority Interest": "Lợi nhuận trước lợi ích thiểu số",
    "Net Income Before Taxes": "Lợi nhuận trước thuế",
    "Income before Taxes": "Thu nhập trước thuế",
    "Income before Discontinued Operations & Extraordinary Items": "Thu nhập trước các khoản đặc biệt",
    "Income Tax - Total": "Thuế TNDN",
    "Income Taxes": "Thuế thu nhập",
    "Income Taxes - Paid/(Reimbursed) - Cash Flow - Supplemental": "Thuế thu nhập đã trả - LCTT",
    "Earnings Per Share - Basic": "EPS cơ bản",
    "EPS - Basic - incl Extraordinary Items, Common - Total": "EPS cơ bản",
    "EPS - Basic - incl Extraordinary, Common - Issue Specific": "EPS cơ bản - Riêng biệt",
    "EPS - Basic - excl Extraordinary Items, Common - Total": "EPS cơ bản không bao gồm khoản đặc biệt",
    "EPS - Basic - excl Extraordinary Items - Normalized - Total": "EPS cơ bản chuẩn hóa",
    "EPS - Basic - excl Exord Items - Normalized - Issue Specific": "EPS cơ bản chuẩn hóa - Riêng biệt",
    "Shares used to calculate Basic EPS - Total": "Số cổ phiếu tính EPS cơ bản",
    "Earnings Per Share - Diluted": "EPS pha loãng",
    "Earnings before Interest & Taxes (EBIT)": "EBIT",
    "Earnings before Interest Taxes Depreciation & Amortization": "EBITDA",
    "Earnings before Interest Tax Depr & Amort & Optg Lease Pymt": "EBITDA bao gồm thuê HĐ",
    "Employees - Full-Time/Full-Time Equivalents - Period End": "Số nhân viên cuối kỳ",
    "Employees - Full-Time/Full-Time Equivalents - Current Date": "Số nhân viên hiện tại",

    # Cash flow (CF)
    "Net Cash From Operating Activities": "Lưu chuyển tiền từ HĐKD",
    "Net Cash from Operating Activities": "Lưu chuyển tiền từ HĐKD",
    "Net Cash From Investing Activities": "Lưu chuyển tiền từ HĐĐT",
    "Net Cash from Investing Activities": "Lưu chuyển tiền từ HĐĐT",
    "Net Cash From Financing Activities": "Lưu chuyển tiền từ HĐTC",
    "Change In Cash": "Thay đổi tiền thuần",
    "Cash At Beginning Of Period": "Tiền đầu kỳ",
    "Cash At End Of Period": "Tiền cuối kỳ",
    "Net Cash - Ending Balance": "Tiền cuối kỳ",
    "Capital Expenditures - Net - Cash Flow": "Chi tiêu vốn ròng",
    "Property Plant & Equipment - Purchased - Cash Flow": "TSCĐ mua - LCTT",
    "Capital Expenditures - Total": "Tổng chi tiêu vốn",
    "Dividends Paid - Cash - Total - Cash Flow": "Cổ tức đã trả - LCTT",
    "Dividends - Common - Cash Paid": "Cổ tức thường đã trả",
    "Dividends Provided/Paid - Common": "Cổ tức thường",
    "Cash Dividends Paid & Common Stock Buyback - Net": "Cổ tức và mua lại cổ phiếu",
    "Common Stock Buyback - Net": "Mua lại cổ phiếu thường",
    "Profit/(Loss) - Starting Line - Cash Flow": "Lãi/Lỗ - LCTT",
    "Free Cash Flow": "Dòng tiền tự do",
    "Free Cash Flow Net of Dividends": "Dòng tiền tự do sau cổ tức",
    "Free Cash Flow to Equity": "Dòng tiền tự do cho cổ đông",
    "Investments excluding Loans - Decrease/(Increase) - CF": "Thay đổi đầu tư - LCTT",
    "Investment Securities Unclassifd - Sold/(Purch) Net Total CF": "Chứng khoán đầu tư bán/mua - LCTT",
    "Investment Securities - Sold/Matured - Unclassified - CF": "Chứng khoán đầu tư bán/đáo hạn - LCTT",
    "Investment Securities - Purchased - Unclassified - Cash Flow": "Chứng khoán đầu tư mua - LCTT",
    "Contract Assets - Decrease/(Increase) - Cash Flow": "Thay đổi tài sản hợp đồng - LCTT",
    "Contract Assets/Liabilities – Net – Cash Flow": "Tài sản/nợ hợp đồng ròng - LCTT",
    "DPS - Common - Gross - Issue - By Announcement Date": "Cổ tức trên cổ phiếu - Gộp",
    "DPS - Common - Net - Issue - By Announcement Date": "Cổ tức trên cổ phiếu - Ròng",
}

CHI_SO_CAN_DOI_KE_TOAN = list(set([
    'Tổng tài sản ngắn hạn', 'Tiền và tương đương tiền', 'Đầu tư ngắn hạn', 'Khoản vay và phải thu ngắn hạn', 
    'Phải thu thương mại', 'Khoản vay ngắn hạn', 'Thuế thu nhập phải thu ngắn hạn', 'Phải thu khác', 'Dự phòng nợ khó đòi',
    'Tổng hàng tồn kho', 'Hàng tồn kho - Nguyên vật liệu', 'Hàng tồn kho - Sản phẩm dở dang', 'Hàng tồn kho - Thành phẩm', 
    'Tài sản ngắn hạn khác', 'Chi phí trả trước ngắn hạn', 'Tổng Non-ngắn hạn tài sản', 'Tài sản cố định ròng', 
    'TSCĐ khác - Ròng', 'Tài sản cố định gốc', 'Tài sản cố định', 'TSCĐ không bao gồm cho thuê', 'Đầu tư dài hạn', 
    'Bất động sản đầu tư - Khấu hao lũy kế', 'Tổng tài sản', 'Tổng nợ phải trả', 'Tổng nợ ngắn hạn', 
    'Phải trả thương mại và trích trước ngắn hạn', 'Phải trả thương mại ngắn hạn', 'Chi phí trích trước ngắn hạn',
    'Nợ ngắn hạn và phần ngắn hạn của nợ dài hạn', 'Nợ và phải trả ngắn hạn', 'Phần ngắn hạn của nợ dài hạn', 
    'Phần ngắn hạn của nợ dài hạn không bao gồm thuê TT', 'Thuế thu nhập phải trả ngắn hạn', 'Tổng nợ ngắn hạn khác', 
    'Dự phòng ngắn hạn', 'Nợ ngắn hạn khác', 'Tổng nợ dài hạn', 'Phải trả bao gồm chi phí trích trước dài hạn',
    'Phải trả thương mại dài hạn', 'Chi phí trích trước dài hạn', 'Nợ không chuyển đổi dài hạn', 
    'Thuế hoãn lại và ưu đãi thuế đầu tư dài hạn', 'Thuế hoãn lại dài hạn', 'Tổng nợ dài hạn khác', 'Dự phòng dài hạn', 
    'Doanh thu hoãn lại dài hạn', 'Tổng vốn cổ đông bao gồm cổ đông thiểu số', 'Tổng vốn cổ phiếu thường',
    'Lợi ích cổ đông thiểu số', 'Tổng lợi nhuận giữ lại', 'Tổng thu nhập toàn diện lũy kế', 'Tổng nợ và vốn chủ sở hữu',
]))

CHI_SO_KET_QUA_KINH_DOANH = list(set([
    'Doanh thu bán hàng và dịch vụ', 'Giảm trừ doanh thu', 'Doanh thu từ hàng hóa và dịch vụ', 'Tổng giá vốn', 
    'Tổng doanh thu gộp từ hoạt động kinh doanh', 'Thu nhập trước thuế', 'Lợi nhuận sau thuế', 
    'Lợi nhuận trước lợi ích thiểu số', 'Lợi nhuận sau lợi ích thiểu số', 'EPS cơ bản', 'EPS cơ bản chuẩn hóa',
]))

CHI_SO_LUU_CHUYEN_TIEN_TE = list(set([
    'Thuần Lưu chuyển tiền from Hoạt động hoạt động', 'Chi tiêu vốn ròng', 'Thay đổi đầu tư - LCTT', 'TSCĐ mua - LCTT', 
    'Tổng chi tiêu vốn', 'Cổ tức đã trả - LCTT', 'Tiền cuối kỳ', 'Cổ tức và mua lại cổ phiếu', 'Lãi/Lỗ - LCTT', 
    'Dòng tiền tự do cho cổ đông', 'Dòng tiền tự do sau cổ tức', 'Dòng tiền tự do',
]))

# ==============================================================
# PHẦN 2: HÀM ETL DÙNG CHUNG (SHARED HELPERS)
# ==============================================================
def vi_translate_indicator(name: str) -> str:
    if name in VI_MAP:
        return VI_MAP[name]
    s = str(name)
    replacements = [
        (r"\bCash & Cash Equivalents\b", "Tiền và tương đương tiền"),
        (r"\bCash & Short Term Investments\b", "Tiền và đầu tư ngắn hạn"),
        (r"\bProperty Plant & Equipment\b", "Tài sản cố định"),
        (r"\bProperty, Plant & Equipment\b", "Tài sản cố định"),
        (r"\bProperty Plant and Equipment\b", "Tài sản cố định"),
        (r"\bAccounts Receivable\b", "Phải thu"),
        (r"\bAccounts Payable\b", "Phải trả"),
        (r"\bShort-?Term Investments\b", "Đầu tư ngắn hạn"),
        (r"\bLong-?Term Debt\b", "Nợ dài hạn"),
        (r"\bShort-?Term Debt\b", "Nợ ngắn hạn"),
        (r"\bIntangible Assets\b", "Tài sản vô hình"),
        (r"\bAccumulated Depreciation\b", "Khấu hao lũy kế"),
        (r"\bNet Income\b", "Lợi nhuận sau thuế"),
        (r"\bGross Profit\b", "Lợi nhuận gộp"),
        (r"\bOperating Income\b", "Lợi nhuận hoạt động"),
        (r"\bOperating Expenses\b", "Chi phí hoạt động"),
        (r"\bCost of Revenue\b", "Giá vốn"),
        (r"\bEarnings Per Share\b", "Thu nhập trên mỗi cổ phiếu"),
        (r"\bCash Flow\b", "Lưu chuyển tiền"),
        (r"\bRetained Earnings\b", "Lợi nhuận giữ lại"),
        (r"\bShareholders['\s]* Equity\b", "Vốn cổ đông"),
        (r"\bCommon Stock\b", "Cổ phiếu thường"),
        (r"\bDividends Paid\b", "Cổ tức trả"),
        (r"\bIncome Tax\b", "Thuế thu nhập"),
        (r"\bTotal\b", "Tổng"),
        (r"\bAssets\b", "tài sản"),
        (r"\bLiabilities\b", "nợ phải trả"),
        (r"\bEquity\b", "vốn chủ sở hữu"),
        (r"\bCurrent\b", "ngắn hạn"),
        (r"\bNon[- ]Current\b", "dài hạn"),
        (r"\bRevenue\b", "Doanh thu"),
        (r"\bNet\b", "Thuần"),
        (r"\bGross\b", "Gộp"),
        (r"\bCash\b", "Tiền"),
        (r"\bInventory\b", "Hàng tồn kho"),
        (r"\bInventories\b", "Hàng tồn kho"),
        (r"\bReceivable\b", "Phải thu"),
        (r"\bReceivables\b", "Phải thu"),
        (r"\bPayable\b", "Phải trả"),
        (r"\bLoans\b", "Khoản vay"),
        (r"\bDebt\b", "Nợ"),
        (r"\bInvestments\b", "Đầu tư"),
        (r"\bDepreciation\b", "Khấu hao"),
        (r"\bAmortization\b", "Phân bổ"),
        (r"\bGoodwill\b", "Lợi thế thương mại"),
        (r"\bExpenses\b", "Chi phí"),
        (r"\bPurchased\b", "Mua"),
        (r"\bSold\b", "Bán"),
        (r"\bTrade\b", "Thương mại"),
        (r"\bOther\b", "Khác"),
        (r"\bPeriod\b", "Kỳ"),
        (r"\bBeginning\b", "Đầu"),
        (r"\bEnd\b", "Cuối"),
        (r"\bOperating\b", "Hoạt động"),
        (r"\bInvesting\b", "Đầu tư"),
        (r"\bFinancing\b", "Tài chính"),
        (r"\bActivities\b", "hoạt động"),
        (r"&", "và"),
        (r"\band\b", "và"),
        (r"- Total", ""),
        (r"/", " "),
    ]
    for pat, rep in replacements: s = re.sub(pat, rep, s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return re.sub(r"\s*-\s*$", "", s) 

def parse_sheet_raw(xl: pd.ExcelFile, sheet_name: str, has_date: bool) -> pd.DataFrame:
    if sheet_name not in xl.sheet_names: return pd.DataFrame()
    df_raw = xl.parse(sheet_name, header=None)
    if df_raw.empty or len(df_raw.columns) < 2: return pd.DataFrame()

    data_start_row = -1
    for idx in range(min(15, len(df_raw))):
        if df_raw.iloc[idx].astype(str).str.contains(r"\.IC$", na=False).any():
            data_start_row = idx
            break
            
    if data_start_row <= 0: return pd.DataFrame()

    raw_headers = df_raw.iloc[data_start_row - 1].values
    clean_headers, counts = [], {}
    for i, h in enumerate(raw_headers):
        h_str = str(h).strip() if pd.notna(h) else f"Unnamed_Col_{i}"
        if h_str in counts:
            counts[h_str] += 1
            clean_headers.append(f"{h_str}_{counts[h_str]}")
        else:
            counts[h_str] = 1
            clean_headers.append(h_str)

    df = df_raw.iloc[data_start_row:].copy().reset_index(drop=True)
    df.columns = clean_headers

    ticker_col, date_col = None, None
    for c in df.columns:
        if df[c].astype(str).str.contains(r"\.IC$", na=False).any():
            ticker_col = c
            break
            
    if has_date:
        for c in df.columns:
            if c != ticker_col and pd.to_datetime(df[c], errors='coerce').notna().sum() > 0:
                date_col = c
                break

    if ticker_col: df = df.rename(columns={ticker_col: "Ticker"})
    if has_date and date_col: df = df.rename(columns={date_col: "Date"})
        
    valid_cols = [c for c in df.columns if c in ["Ticker", "Date"] or not c.startswith("Unnamed_Col_")]
    df = df[valid_cols]

    if has_date and "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]) 
        
    for c in df.columns:
        if c not in ["Ticker", "Date"]:
            if sheet_name != "COMP":
                df[c] = df[c].replace(["NULL", "null", "", "NaN"], np.nan)
                df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def load_and_merge(xl: pd.ExcelFile, prefix: str) -> pd.DataFrame:
    sheets = [s for s in xl.sheet_names if re.fullmatch(fr"{prefix}\d+", s)]
    parts = []
    for sh in sorted(sheets, key=lambda x: int(re.findall(r"\d+", x)[0])):
        df = parse_sheet_raw(xl, sh, has_date=True)
        if not df.empty: parts.append(df)
            
    if not parts: return pd.DataFrame()

    merged = parts[0]
    for p in parts[1:]:
        new_cols = [c for c in p.columns if c in ["Ticker", "Date"] or c not in merged.columns]
        if len(new_cols) > 2:
            merged = merged.merge(p[new_cols], on=["Ticker", "Date"], how="outer")

    merged = merged.rename(columns={c: vi_translate_indicator(c) for c in merged.columns if c not in ["Ticker", "Date"]})
    return merged.rename(columns={"Ticker": "Mã CK", "Date": "Ngày"})

def to_long(df: pd.DataFrame, report_name: str, has_date: bool = True) -> pd.DataFrame:
    id_vars = ["Mã CK", "Ngày"] if has_date else ["Mã CK"]
    value_cols = [c for c in df.columns if c not in id_vars]
    long_df = df.melt(id_vars=id_vars, value_vars=value_cols, var_name="Chỉ số", value_name="Giá trị")
    
    if has_date:
        long_df.insert(2, "Báo cáo", report_name)
        long_df = long_df.dropna(subset=['Ngày'])
    else:
        long_df.insert(1, "Báo cáo", report_name)
    return long_df

def filter_data_by_indicators(df, indicators_list, report_name):
    df_filtered = df[df['Chỉ số'].isin(indicators_list)].copy()
    df_filtered['Báo cáo'] = report_name
    df_filtered['Date_Temp'] = pd.to_datetime(df_filtered['Ngày'], format='%d/%m/%Y', errors='coerce')
    df_filtered = df_filtered.sort_values(by=['Mã CK', 'Date_Temp']).reset_index(drop=True)
    return df_filtered.drop(columns=['Date_Temp'])[['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']]

def pivot_helper(df_data, needed_cols):
    df_subset = df_data[df_data['Chỉ số'].isin(needed_cols)][['Mã CK', 'Ngày', 'Chỉ số', 'Giá trị']].copy()
    df_subset['Giá trị'] = pd.to_numeric(df_subset['Giá trị'], errors='coerce')
    calc_table = df_subset.pivot_table(index=['Mã CK', 'Ngày'], columns='Chỉ số', values='Giá trị').reset_index()
    for col in needed_cols:
        if col not in calc_table.columns: calc_table[col] = np.nan
    calc_table['Date_Temp'] = pd.to_datetime(calc_table['Ngày'], format='%d/%m/%Y', errors='coerce')
    return calc_table.sort_values(by=['Mã CK', 'Date_Temp']).reset_index(drop=True)

def melt_helper(calc_table, result_metrics, report_name):
    calc_table.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_res = calc_table.melt(id_vars=['Mã CK', 'Ngày'], value_vars=result_metrics, var_name='Chỉ số', value_name='Giá trị')
    df_res['Báo cáo'] = report_name
    return df_res.dropna(subset=['Giá trị'])[['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']]

# ==============================================================
# PHẦN 3: HÀM TÍNH TOÁN THEO NHÓM (DYNAMICAL BRANCHING)
# ==============================================================
def calculate_Tong_Quan_CSTC(df_comp, df_price, df_data):
    new_metrics = []
    df_industry = df_comp[df_comp['Chỉ số'].isin(['Ngành GICS', 'Lĩnh vực GICS'])].copy()
    df_industry['Ngày'] = np.nan 
    df_industry['Báo cáo'] = 'Tổng quan CSTC'
    new_metrics.append(df_industry[['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']])

    if df_price.empty: return pd.concat(new_metrics, ignore_index=True)

    df_shares = df_data[df_data['Chỉ số'] == 'Tổng cổ phiếu thường đang lưu hành'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Shares'})
    df_close = df_price[df_price['Chỉ số'] == 'Giá đóng cửa'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Price'})
    
    df_mcap = pd.merge(df_shares, df_close, on=['Mã CK', 'Ngày'], how='left')
    df_mcap_out = df_mcap.copy()
    df_mcap_out['Giá trị'] = pd.to_numeric(df_mcap_out['Shares'], errors='coerce') * pd.to_numeric(df_mcap_out['Price'], errors='coerce')
    df_mcap_out['Chỉ số'], df_mcap_out['Báo cáo'] = 'Vốn hóa thị trường', 'Tổng quan CSTC'
    
    df_mcap_out = df_mcap_out.dropna(subset=['Giá trị'])
    new_metrics.append(df_mcap_out[['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']])
    return pd.concat(new_metrics, ignore_index=True)

def calculate_Nhom_CSTC_Chung(df_price, df_data):
    new_metrics = []
    for ind in ['EBIT', 'EBITDA']:
        df_temp = df_data[df_data['Chỉ số'] == ind][['Mã CK', 'Ngày', 'Giá trị']].copy()
        df_temp['Báo cáo'], df_temp['Chỉ số'] = 'Nhóm CSTC chung', ind
        df_temp = df_temp.dropna(subset=['Giá trị'])
        new_metrics.append(df_temp[['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']])

    if df_price.empty: return pd.concat(new_metrics, ignore_index=True)

    df_shares = df_data[df_data['Chỉ số'] == 'Tổng cổ phiếu thường đang lưu hành'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Shares'})
    df_close = df_price[df_price['Chỉ số'] == 'Giá đóng cửa'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Price'})
    df_mcap = pd.merge(df_shares, df_close, on=['Mã CK', 'Ngày'], how='left')
    df_mcap['Market Cap'] = pd.to_numeric(df_mcap['Shares'], errors='coerce') * pd.to_numeric(df_mcap['Price'], errors='coerce')
    
    df_netdebt = df_data[df_data['Chỉ số'] == 'Nợ ròng'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Net Debt'})
    df_netdebt['Net Debt'] = pd.to_numeric(df_netdebt['Net Debt'], errors='coerce')
    
    df_ev = pd.merge(df_mcap[['Mã CK', 'Ngày', 'Market Cap']], df_netdebt, on=['Mã CK', 'Ngày'], how='left')
    df_ev['Giá trị'] = df_ev['Market Cap'] + df_ev['Net Debt']
    df_ev['Chỉ số'], df_ev['Báo cáo'] = 'Giá trị doanh nghiệp (EV)', 'Nhóm CSTC chung'
    
    df_ev_clean = df_ev.dropna(subset=['Giá trị']).copy()
    new_metrics.append(df_ev_clean[['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']])
    return pd.concat(new_metrics, ignore_index=True)

def calculate_Nhom_Dinh_Gia(df_price, df_data, period_type):
    if period_type == 'A':
        needed = ['EPS cơ bản', 'Vốn cổ đông thường', 'Tổng cổ phiếu thường đang lưu hành', 'Doanh thu từ hoạt động kinh doanh', 'Cổ tức trên cổ phiếu - Gộp', 'Nợ ròng', 'EBIT', 'EBITDA']
        calc_table = pivot_helper(df_data, needed)
        if not df_price.empty:
            df_close = df_price[df_price['Chỉ số'] == 'Giá đóng cửa'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Price'})
            df_close['Price'] = pd.to_numeric(df_close['Price'], errors='coerce')
            calc_table = pd.merge(calc_table, df_close, on=['Mã CK', 'Ngày'], how='left')
        if 'Price' not in calc_table.columns: calc_table['Price'] = np.nan

        calc_table['EPS (Proxy TTM)'] = calc_table['EPS cơ bản']
        calc_table['BVPS'] = calc_table['Vốn cổ đông thường'] / calc_table['Tổng cổ phiếu thường đang lưu hành']
        calc_table['P/E'] = calc_table['Price'] / calc_table['EPS cơ bản']
        calc_table['P/B'] = calc_table['Price'] / calc_table['BVPS']
        calc_table['P/S'] = calc_table['Price'] / (calc_table['Doanh thu từ hoạt động kinh doanh'] / calc_table['Tổng cổ phiếu thường đang lưu hành'])
        calc_table['Tỷ suất cổ tức'] = calc_table['Cổ tức trên cổ phiếu - Gộp'] / calc_table['Price']
        ev = (calc_table['Price'] * calc_table['Tổng cổ phiếu thường đang lưu hành']) + calc_table['Nợ ròng']
        calc_table['EV/EBIT'] = ev / calc_table['EBIT']
        calc_table['EV/EBITDA'] = ev / calc_table['EBITDA']
        
        return melt_helper(calc_table, ['EPS (Proxy TTM)', 'BVPS', 'P/E', 'P/B', 'P/S', 'Tỷ suất cổ tức', 'EV/EBIT', 'EV/EBITDA'], 'Nhóm Định giá')
    else:
        # QUÝ
        needed = ['Lợi nhuận sau lợi ích thiểu số', 'Số cổ phiếu tính EPS cơ bản', 'Vốn cổ đông thường', 'Tổng cổ phiếu thường đang lưu hành', 'Doanh thu từ hoạt động kinh doanh', 'Cổ tức trên cổ phiếu - Gộp', 'Nợ ròng', 'EBIT', 'EBITDA']
        calc_table = pivot_helper(df_data, needed)
        
        if not df_price.empty:
            df_close = df_price[df_price['Chỉ số'] == 'Giá đóng cửa'][['Mã CK', 'Ngày', 'Giá trị']].rename(columns={'Giá trị': 'Price'})
            df_close['Price'] = pd.to_numeric(df_close['Price'], errors='coerce')
            calc_table = pd.merge(calc_table, df_close, on=['Mã CK', 'Ngày'], how='left')
        if 'Price' not in calc_table.columns: calc_table['Price'] = np.nan

        for col in ['Lợi nhuận sau lợi ích thiểu số', 'Doanh thu từ hoạt động kinh doanh', 'Cổ tức trên cổ phiếu - Gộp', 'EBIT', 'EBITDA']:
            calc_table[f'{col}_TTM'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(window=4, min_periods=4).sum())

        calc_table['EPS TTM'] = calc_table['Lợi nhuận sau lợi ích thiểu số_TTM'] / calc_table['Số cổ phiếu tính EPS cơ bản']
        calc_table['BVPS'] = calc_table['Vốn cổ đông thường'] / calc_table['Tổng cổ phiếu thường đang lưu hành']
        calc_table['P/E'] = calc_table['Price'] / calc_table['EPS TTM']
        calc_table['P/B'] = calc_table['Price'] / calc_table['BVPS']
        calc_table['P/S'] = calc_table['Price'] / (calc_table['Doanh thu từ hoạt động kinh doanh_TTM'] / calc_table['Tổng cổ phiếu thường đang lưu hành'])
        calc_table['Tỷ suất cổ tức'] = calc_table['Cổ tức trên cổ phiếu - Gộp_TTM'] / calc_table['Price']
        
        ev = (calc_table['Price'] * calc_table['Tổng cổ phiếu thường đang lưu hành']) + calc_table['Nợ ròng']
        calc_table['EV/EBIT'] = ev / calc_table['EBIT_TTM']
        calc_table['EV/EBITDA'] = ev / calc_table['EBITDA_TTM']
        
        return melt_helper(calc_table, ['EPS TTM', 'BVPS', 'P/E', 'P/B', 'P/S', 'Tỷ suất cổ tức', 'EV/EBIT', 'EV/EBITDA'], 'Nhóm Định giá')

def calculate_Nhom_Sinh_Loi(df_data, period_type):
    if period_type == 'A':
        needed = ['Tổng lợi nhuận gộp', 'Doanh thu từ hoạt động kinh doanh', 'EBIT', 'EBITDA', 'Lợi nhuận sau thuế', 'Lợi nhuận sau lợi ích thiểu số', 'Vốn cổ đông thường', 'Tổng vốn dài hạn', 'Tổng tài sản']
        calc_table = pivot_helper(df_data, needed)

        calc_table['Tỷ suất lợi nhuận gộp biên'] = calc_table['Tổng lợi nhuận gộp'] / calc_table['Doanh thu từ hoạt động kinh doanh']
        calc_table['Tỷ lệ lãi EBIT'] = calc_table['EBIT'] / calc_table['Doanh thu từ hoạt động kinh doanh']
        calc_table['Tỷ lệ lãi EBITDA'] = calc_table['EBITDA'] / calc_table['Doanh thu từ hoạt động kinh doanh']
        calc_table['Tỷ suất sinh lợi trên doanh thu thuần'] = calc_table['Lợi nhuận sau thuế'] / calc_table['Doanh thu từ hoạt động kinh doanh']

        for col in ['Vốn cổ đông thường', 'Tổng vốn dài hạn', 'Tổng tài sản']:
            calc_table[f'{col}_avg'] = (calc_table[col] + calc_table.groupby('Mã CK')[col].shift(1)) / 2

        calc_table['Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)'] = calc_table['Lợi nhuận sau lợi ích thiểu số'] / calc_table['Vốn cổ đông thường_avg']
        calc_table['Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)'] = calc_table['EBIT'] / calc_table['Tổng vốn dài hạn_avg']
        calc_table['Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)'] = calc_table['Lợi nhuận sau thuế'] / calc_table['Tổng tài sản_avg']

        return melt_helper(calc_table, ['Tỷ suất lợi nhuận gộp biên', 'Tỷ lệ lãi EBIT', 'Tỷ lệ lãi EBITDA', 'Tỷ suất sinh lợi trên doanh thu thuần', 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', 'Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)', 'Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)'], 'Nhóm Sinh lợi')
    else:
        # QUÝ
        needed = ['Tổng lợi nhuận gộp', 'Doanh thu bán hàng và dịch vụ', 'Doanh thu từ hoạt động kinh doanh', 'EBIT', 'EBITDA', 'Lợi nhuận sau thuế', 'Tổng vốn cổ đông thuộc công ty mẹ', 'Tổng vốn dài hạn', 'Tổng tài sản']
        calc_table = pivot_helper(df_data, needed)
        
        calc_table['Doanh thu thuần_best'] = calc_table['Doanh thu bán hàng và dịch vụ'].fillna(calc_table['Doanh thu từ hoạt động kinh doanh'])
        
        for col in ['Tổng lợi nhuận gộp', 'Doanh thu thuần_best', 'EBIT', 'EBITDA', 'Lợi nhuận sau thuế']:
            calc_table[f'{col}_TTM'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(window=4, min_periods=4).sum())
            
        for col in ['Tổng vốn cổ đông thuộc công ty mẹ', 'Tổng vốn dài hạn', 'Tổng tài sản']:
            calc_table[f'{col}_avg'] = (calc_table[col] + calc_table.groupby('Mã CK')[col].shift(1)) / 2
            
        calc_table['Tỷ suất lợi nhuận gộp biên'] = calc_table['Tổng lợi nhuận gộp_TTM'] / calc_table['Doanh thu thuần_best_TTM']
        calc_table['Tỷ lệ lãi EBIT'] = calc_table['EBIT_TTM'] / calc_table['Doanh thu thuần_best_TTM']
        calc_table['Tỷ lệ lãi EBITDA'] = calc_table['EBITDA_TTM'] / calc_table['Doanh thu thuần_best_TTM']
        calc_table['Tỷ suất sinh lợi trên doanh thu thuần (NPM)'] = calc_table['Lợi nhuận sau thuế_TTM'] / calc_table['Doanh thu thuần_best_TTM']
        calc_table['Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)'] = calc_table['Lợi nhuận sau thuế_TTM'] / calc_table['Tổng vốn cổ đông thuộc công ty mẹ_avg']
        calc_table['Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)'] = calc_table['EBIT_TTM'] / calc_table['Tổng vốn dài hạn_avg']
        calc_table['Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)'] = calc_table['Lợi nhuận sau thuế_TTM'] / calc_table['Tổng tài sản_avg']
        
        return melt_helper(calc_table, ['Tỷ suất lợi nhuận gộp biên', 'Tỷ lệ lãi EBIT', 'Tỷ lệ lãi EBITDA', 'Tỷ suất sinh lợi trên doanh thu thuần (NPM)', 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', 'Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)', 'Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)'], 'Nhóm Sinh lợi')

def calculate_Nhom_Tang_Truong(df_data, period_type):
    needed = ['Doanh thu từ hoạt động kinh doanh', 'Tổng lợi nhuận gộp', 'Thu nhập trước thuế', 'Lợi nhuận sau lợi ích thiểu số', 'Tổng tài sản', 'Tổng nợ dài hạn', 'Tổng nợ phải trả', 'Vốn cổ đông thường', 'Vốn cổ phiếu thường thuộc cổ đông công ty mẹ']
    calc_table = pivot_helper(df_data, needed)

    shift_val = 1 if period_type == 'A' else 4
    
    if period_type == 'Q':
        for col in ['Doanh thu từ hoạt động kinh doanh', 'Tổng lợi nhuận gộp', 'Thu nhập trước thuế', 'Lợi nhuận sau lợi ích thiểu số']:
            calc_table[f'{col}_Calc'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(4, min_periods=4).sum())
    else:
        for col in ['Doanh thu từ hoạt động kinh doanh', 'Tổng lợi nhuận gộp', 'Thu nhập trước thuế', 'Lợi nhuận sau lợi ích thiểu số']:
            calc_table[f'{col}_Calc'] = calc_table[col]

    def compute_growth(col_name, new_col_name):
        prev = calc_table.groupby('Mã CK')[col_name].shift(shift_val)
        calc_table[new_col_name] = ((calc_table[col_name] - prev) / prev.abs()).where(prev > 0 if period_type == 'A' else prev != 0, np.nan)

    compute_growth('Doanh thu từ hoạt động kinh doanh_Calc', 'Tăng trưởng doanh thu thuần')
    compute_growth('Tổng lợi nhuận gộp_Calc', 'Tăng trưởng lợi nhuận gộp')
    compute_growth('Thu nhập trước thuế_Calc', 'Tăng trưởng lợi nhuận trước thuế')
    compute_growth('Lợi nhuận sau lợi ích thiểu số_Calc', 'Tăng trưởng lợi nhuận sau thuế của CĐ công ty mẹ')
    compute_growth('Tổng tài sản', 'Tăng trưởng tổng tài sản')
    compute_growth('Tổng nợ dài hạn', 'Tăng trưởng nợ dài hạn')
    compute_growth('Tổng nợ phải trả', 'Tăng trưởng nợ phải trả')
    compute_growth('Vốn cổ đông thường', 'Tăng trưởng vốn chủ sở hữu')
    
    metrics = ['Tăng trưởng doanh thu thuần', 'Tăng trưởng lợi nhuận gộp', 'Tăng trưởng lợi nhuận trước thuế', 'Tăng trưởng lợi nhuận sau thuế của CĐ công ty mẹ', 'Tăng trưởng tổng tài sản', 'Tăng trưởng nợ dài hạn', 'Tăng trưởng nợ phải trả', 'Tăng trưởng vốn chủ sở hữu']
    if period_type == 'Q':
        compute_growth('Vốn cổ phiếu thường thuộc cổ đông công ty mẹ', 'Tăng trưởng vốn điều lệ')
        metrics.append('Tăng trưởng vốn điều lệ')
        
    return melt_helper(calc_table, metrics, 'Nhóm Tăng trưởng')

def calculate_Nhom_Thanh_Khoan(df_data, period_type):
    if period_type == 'A':
        needed = ['Tiền và tương đương tiền', 'Tiền và đầu tư ngắn hạn', 'Tổng nợ ngắn hạn', 'Tổng tài sản ngắn hạn', 'Tổng hàng tồn kho', 'EBIT', 'Thu nhập trước thuế']
        calc_table = pivot_helper(df_data, needed)

        calc_table['Tỷ số thanh toán bằng tiền mặt (Cash Ratio)'] = calc_table['Tiền và tương đương tiền'] / calc_table['Tổng nợ ngắn hạn']
        calc_table['Tỷ số thanh toán nhanh (Quick Ratio)'] = (calc_table['Tổng tài sản ngắn hạn'] - calc_table['Tổng hàng tồn kho'].fillna(0)) / calc_table['Tổng nợ ngắn hạn']
        calc_table['Tỷ số thanh toán nhanh (Loại HTK & PT)'] = calc_table['Tiền và đầu tư ngắn hạn'].fillna(calc_table['Tiền và tương đương tiền']) / calc_table['Tổng nợ ngắn hạn']
        calc_table['Tỷ số thanh toán hiện hành (Current Ratio)'] = calc_table['Tổng tài sản ngắn hạn'] / calc_table['Tổng nợ ngắn hạn']
        
        implied_interest = calc_table['EBIT'] - calc_table['Thu nhập trước thuế']
        calc_table['Khả năng thanh toán lãi vay (ICR)'] = np.where(implied_interest > 0, calc_table['EBIT'] / implied_interest, np.nan)
        return melt_helper(calc_table, ['Tỷ số thanh toán bằng tiền mặt (Cash Ratio)', 'Tỷ số thanh toán nhanh (Quick Ratio)', 'Tỷ số thanh toán nhanh (Loại HTK & PT)', 'Tỷ số thanh toán hiện hành (Current Ratio)', 'Khả năng thanh toán lãi vay (ICR)'], 'Nhóm Thanh khoản')
    else:
        needed = ['Tiền và tương đương tiền', 'Tổng nợ ngắn hạn', 'Tổng khoản vay và phải thu', 'Khoản vay và phải thu ngắn hạn', 'Tổng tài sản ngắn hạn', 'Tổng hàng tồn kho', 'EBIT', 'Thu nhập trước thuế']
        calc_table = pivot_helper(df_data, needed)
        
        for col in ['EBIT', 'Thu nhập trước thuế']:
            calc_table[f'{col}_TTM'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(4, min_periods=4).sum())
            
        ar_col = calc_table['Tổng khoản vay và phải thu'] if 'Tổng khoản vay và phải thu' in calc_table.columns and calc_table['Tổng khoản vay và phải thu'].notna().any() else calc_table.get('Khoản vay và phải thu ngắn hạn', 0)
        inv_filled = calc_table['Tổng hàng tồn kho'].fillna(0) if 'Tổng hàng tồn kho' in calc_table.columns else 0
        
        calc_table['Tỷ số thanh toán bằng tiền mặt (Cash Ratio)'] = calc_table['Tiền và tương đương tiền'] / calc_table['Tổng nợ ngắn hạn']
        calc_table['Tỷ số thanh toán nhanh (Quick Ratio)'] = (calc_table['Tiền và tương đương tiền'] + ar_col) / calc_table['Tổng nợ ngắn hạn']
        calc_table['Tỷ số thanh toán nhanh (Acid-test Ratio)'] = (calc_table['Tổng tài sản ngắn hạn'] - inv_filled - ar_col) / calc_table['Tổng nợ ngắn hạn']
        calc_table['Tỷ số thanh toán hiện hành (Current Ratio)'] = calc_table['Tổng tài sản ngắn hạn'] / calc_table['Tổng nợ ngắn hạn']
        
        implied_interest_ttm = calc_table['EBIT_TTM'] - calc_table['Thu nhập trước thuế_TTM']
        calc_table['Khả năng thanh toán lãi vay (ICR)'] = np.where(implied_interest_ttm > 0, calc_table['EBIT_TTM'] / implied_interest_ttm, np.nan)
        
        return melt_helper(calc_table, ['Tỷ số thanh toán bằng tiền mặt (Cash Ratio)', 'Tỷ số thanh toán nhanh (Quick Ratio)', 'Tỷ số thanh toán nhanh (Acid-test Ratio)', 'Tỷ số thanh toán hiện hành (Current Ratio)', 'Khả năng thanh toán lãi vay (ICR)'], 'Nhóm Thanh khoản')

def calculate_Nhom_Hieu_Qua_Hoat_Dong(df_data, period_type):
    if period_type == 'A':
        needed = ['Doanh thu từ hoạt động kinh doanh', 'Giá vốn doanh thu hoạt động', 'Phải thu thương mại', 'Khoản vay và phải thu ngắn hạn', 'Tổng hàng tồn kho', 'Phải trả thương mại ngắn hạn', 'Tổng phải trả thương mại', 'Tài sản cố định ròng', 'Tổng tài sản cố định ròng', 'Tổng tài sản', 'Vốn cổ đông thường']
        calc_table = pivot_helper(df_data, needed)

        calc_table['AR_best'] = calc_table['Phải thu thương mại'].fillna(calc_table['Khoản vay và phải thu ngắn hạn'])
        calc_table['AP_best'] = calc_table['Phải trả thương mại ngắn hạn'].fillna(calc_table['Tổng phải trả thương mại'])
        calc_table['FA_best'] = calc_table['Tài sản cố định ròng'].fillna(calc_table['Tổng tài sản cố định ròng'])

        for col in ['AR_best', 'Tổng hàng tồn kho', 'AP_best', 'FA_best', 'Tổng tài sản', 'Vốn cổ đông thường']:
            calc_table[f'{col}_avg'] = (calc_table[col] + calc_table.groupby('Mã CK')[col].shift(1)) / 2

        rev, cogs = calc_table['Doanh thu từ hoạt động kinh doanh'], calc_table['Giá vốn doanh thu hoạt động']
        calc_table['Vòng quay phải thu khách hàng'] = rev / calc_table['AR_best_avg']
        calc_table['Thời gian thu tiền khách hàng bình quân (DSO)'] = 365 / calc_table['Vòng quay phải thu khách hàng']
        calc_table['Vòng quay hàng tồn kho'] = cogs / calc_table['Tổng hàng tồn kho_avg']
        calc_table['Thời gian tồn kho bình quân (DIO)'] = 365 / calc_table['Vòng quay hàng tồn kho']
        calc_table['Vòng quay phải trả nhà cung cấp'] = cogs / calc_table['AP_best_avg']
        calc_table['Thời gian trả tiền nhà cung cấp bình quân (DPO)'] = 365 / calc_table['Vòng quay phải trả nhà cung cấp']
        calc_table['Vòng quay tài sản cố định'] = rev / calc_table['FA_best_avg']
        calc_table['Vòng quay tổng tài sản'] = rev / calc_table['Tổng tài sản_avg']
        calc_table['Vòng quay vốn chủ sở hữu'] = rev / calc_table['Vốn cổ đông thường_avg']
        return melt_helper(calc_table, ['Vòng quay phải thu khách hàng', 'Thời gian thu tiền khách hàng bình quân (DSO)', 'Vòng quay hàng tồn kho', 'Thời gian tồn kho bình quân (DIO)', 'Vòng quay phải trả nhà cung cấp', 'Thời gian trả tiền nhà cung cấp bình quân (DPO)', 'Vòng quay tài sản cố định', 'Vòng quay tổng tài sản', 'Vòng quay vốn chủ sở hữu'], 'Nhóm Hiệu quả Hoạt động')
    else:
        needed = ['Doanh thu từ hoạt động kinh doanh', 'Tổng giá vốn', 'Tổng khoản vay và phải thu', 'Tổng hàng tồn kho', 'Tổng phải trả thương mại', 'Tài sản cố định ròng', 'Tổng tài sản', 'Vốn cổ đông thường']
        calc_table = pivot_helper(df_data, needed)
        
        for col in ['Doanh thu từ hoạt động kinh doanh', 'Tổng giá vốn']:
            calc_table[f'{col}_TTM'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(4, min_periods=4).sum())
            
        for col in ['Tổng khoản vay và phải thu', 'Tổng hàng tồn kho', 'Tổng phải trả thương mại', 'Tài sản cố định ròng', 'Tổng tài sản', 'Vốn cổ đông thường']:
            calc_table[f'{col}_avg'] = (calc_table[col] + calc_table.groupby('Mã CK')[col].shift(1)) / 2
            
        rev_ttm, cogs_ttm = calc_table['Doanh thu từ hoạt động kinh doanh_TTM'], calc_table['Tổng giá vốn_TTM']
        
        calc_table['Vòng quay phải thu khách hàng'] = rev_ttm / calc_table['Tổng khoản vay và phải thu_avg']
        calc_table['Thời gian thu tiền khách hàng bình quân (DSO)'] = 365 / calc_table['Vòng quay phải thu khách hàng']
        calc_table['Vòng quay hàng tồn kho'] = cogs_ttm / calc_table['Tổng hàng tồn kho_avg']
        calc_table['Thời gian tồn kho bình quân (DIO)'] = 365 / calc_table['Vòng quay hàng tồn kho']
        calc_table['Vòng quay phải trả nhà cung cấp'] = cogs_ttm / calc_table['Tổng phải trả thương mại_avg']
        calc_table['Thời gian trả tiền nhà cung cấp bình quân (DPO)'] = 365 / calc_table['Vòng quay phải trả nhà cung cấp']
        calc_table['Vòng quay tài sản cố định'] = rev_ttm / calc_table['Tài sản cố định ròng_avg']
        calc_table['Vòng quay tổng tài sản'] = rev_ttm / calc_table['Tổng tài sản_avg']
        calc_table['Vòng quay vốn chủ sở hữu'] = rev_ttm / calc_table['Vốn cổ đông thường_avg']
        return melt_helper(calc_table, ['Vòng quay phải thu khách hàng', 'Thời gian thu tiền khách hàng bình quân (DSO)', 'Vòng quay hàng tồn kho', 'Thời gian tồn kho bình quân (DIO)', 'Vòng quay phải trả nhà cung cấp', 'Thời gian trả tiền nhà cung cấp bình quân (DPO)', 'Vòng quay tài sản cố định', 'Vòng quay tổng tài sản', 'Vòng quay vốn chủ sở hữu'], 'Nhóm Hiệu quả Hoạt động')

def calculate_Nhom_Don_Bay_Tai_Chinh(df_data):
    # Logic cho cả năm và quý giống hệt nhau
    needed = ['Tổng nợ ngắn hạn', 'Tổng nợ phải trả', 'Tổng nợ', 'Tổng tài sản', 'Vốn cổ đông thường']
    calc_table = pivot_helper(df_data, needed)

    calc_table['Nợ NH / Tổng nợ PT'] = calc_table['Tổng nợ ngắn hạn'] / calc_table['Tổng nợ phải trả']
    calc_table['Nợ vay / Tổng TS'] = calc_table['Tổng nợ'] / calc_table['Tổng tài sản']
    calc_table['Nợ (PT) / Tổng TS'] = calc_table['Tổng nợ phải trả'] / calc_table['Tổng tài sản']
    calc_table['VCS / Tổng TS'] = calc_table['Vốn cổ đông thường'] / calc_table['Tổng tài sản']
    calc_table['Nợ NH / VCS'] = calc_table['Tổng nợ ngắn hạn'] / calc_table['Vốn cổ đông thường']
    calc_table['Nợ vay / VCS'] = calc_table['Tổng nợ'] / calc_table['Vốn cổ đông thường']
    calc_table['Nợ (PT) / VCS'] = calc_table['Tổng nợ phải trả'] / calc_table['Vốn cổ đông thường']

    return melt_helper(calc_table, ['Nợ NH / Tổng nợ PT', 'Nợ vay / Tổng TS', 'Nợ (PT) / Tổng TS', 'VCS / Tổng TS', 'Nợ NH / VCS', 'Nợ vay / VCS', 'Nợ (PT) / VCS'], 'Nhóm Đòn bẩy Tài chính')

def calculate_Nhom_Dong_Tien(df_data, period_type):
    cfo_names = ['Thuần Lưu chuyển tiền from Hoạt động hoạt động', 'Net Cash Flow from Operating Activities', 'Lưu chuyển tiền từ HĐKD']
    existing_cols = df_data['Chỉ số'].unique()
    cfo_col = next((name for name in cfo_names if name in existing_cols), 'Lưu chuyển tiền từ HĐKD')

    if period_type == 'A':
        needed = [cfo_col, 'Doanh thu từ hoạt động kinh doanh', 'Tổng nợ ngắn hạn', 'Tổng tài sản', 'Vốn cổ đông thường', 'Lợi nhuận sau thuế', 'Tổng nợ', 'Tổng cổ phiếu thường đang lưu hành', 'Tiền và tương đương tiền', 'Tổng nợ phải trả']
        calc_table = pivot_helper(df_data, needed)
        cfo = calc_table[cfo_col]

        calc_table['CFO / Doanh thu thuần'] = cfo / calc_table['Doanh thu từ hoạt động kinh doanh']
        calc_table['CFO / Nợ ngắn hạn'] = cfo / calc_table['Tổng nợ ngắn hạn'] 
        calc_table['CFO / Tổng tài sản'] = cfo / calc_table['Tổng tài sản']
        calc_table['CFO / Vốn CSH'] = cfo / calc_table['Vốn cổ đông thường']
        calc_table['CFO / Lợi nhuận thuần'] = cfo / calc_table['Lợi nhuận sau thuế']
        calc_table['Khả năng TT nợ từ CFO'] = cfo / calc_table['Tổng nợ']
        calc_table['CPS'] = cfo / calc_table['Tổng cổ phiếu thường đang lưu hành']

        avg_assets = (calc_table['Tổng tài sản'] + calc_table.groupby('Mã CK')['Tổng tài sản'].shift(1)) / 2
        calc_table['Tỷ lệ dồn tích - CF method'] = (calc_table['Lợi nhuận sau thuế'] - cfo) / avg_assets

        calc_table['NOA_Temp'] = calc_table['Tổng tài sản'] - calc_table['Tiền và tương đương tiền'].fillna(0) - calc_table['Tổng nợ phải trả'] + calc_table['Tổng nợ'].fillna(0)
        calc_table['Tỷ lệ dồn tích - BS method'] = (calc_table['NOA_Temp'] - calc_table.groupby('Mã CK')['NOA_Temp'].shift(1)) / avg_assets

        return melt_helper(calc_table, ['CFO / Doanh thu thuần', 'CFO / Nợ ngắn hạn', 'CFO / Tổng tài sản', 'CFO / Vốn CSH', 'CFO / Lợi nhuận thuần', 'Khả năng TT nợ từ CFO', 'CPS', 'Tỷ lệ dồn tích - CF method', 'Tỷ lệ dồn tích - BS method'], 'Nhóm Chỉ số Dòng tiền')
    else:
        needed = [cfo_col, 'Doanh thu từ hoạt động kinh doanh', 'Tổng nợ ngắn hạn', 'Tiền cuối kỳ', 'Lợi nhuận sau thuế', 'Tổng tài sản', 'Vốn cổ đông thường', 'Tổng nợ phải trả', 'Số cổ phiếu tính EPS cơ bản']
        calc_table = pivot_helper(df_data, needed)

        for col in [cfo_col, 'Doanh thu từ hoạt động kinh doanh', 'Lợi nhuận sau thuế']:
            calc_table[f'{col}_TTM'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(4, min_periods=4).sum())

        cfo_ttm = calc_table[f'{cfo_col}_TTM']
        calc_table['Tổng tài sản_avg'] = (calc_table['Tổng tài sản'] + calc_table.groupby('Mã CK')['Tổng tài sản'].shift(1)) / 2

        calc_table['CFO / Doanh thu thuần'] = cfo_ttm / calc_table['Doanh thu từ hoạt động kinh doanh_TTM']
        calc_table['Khả năng chi trả nợ ngắn hạn từ CFO'] = cfo_ttm / calc_table['Tổng nợ ngắn hạn']
        calc_table['Khả năng chi trả nợ ngắn hạn từ tiền cuối kỳ'] = calc_table['Tiền cuối kỳ'] / calc_table['Tổng nợ ngắn hạn']
        
        ni_ttm = calc_table['Lợi nhuận sau thuế_TTM']
        calc_table['Tỷ lệ dồn tích - Phương pháp Cân đối kế toán'] = (ni_ttm - cfo_ttm) / calc_table['Tổng tài sản_avg']
        calc_table['Tỷ lệ dồn tích - Phương pháp Dòng tiền'] = (ni_ttm - cfo_ttm) / ni_ttm
        
        calc_table['CFO / Tổng tài sản'] = cfo_ttm / calc_table['Tổng tài sản']
        calc_table['CFO / VCSH'] = cfo_ttm / calc_table['Vốn cổ đông thường']
        calc_table['CFO / Lợi nhuận thuần'] = cfo_ttm / ni_ttm
        calc_table['Khả năng thanh toán nợ từ CFO'] = cfo_ttm / calc_table['Tổng nợ phải trả']
        calc_table['CPS'] = cfo_ttm / calc_table['Số cổ phiếu tính EPS cơ bản']

        return melt_helper(calc_table, ['CFO / Doanh thu thuần', 'Khả năng chi trả nợ ngắn hạn từ CFO', 'Khả năng chi trả nợ ngắn hạn từ tiền cuối kỳ', 'Tỷ lệ dồn tích - Phương pháp Cân đối kế toán', 'Tỷ lệ dồn tích - Phương pháp Dòng tiền', 'CFO / Tổng tài sản', 'CFO / VCSH', 'CFO / Lợi nhuận thuần', 'Khả năng thanh toán nợ từ CFO', 'CPS'], 'Nhóm Dòng tiền')

def calculate_Nhom_Co_Cau_Chi_Phi(df_data, period_type):
    if period_type == 'A':
        needed = ['Giá vốn doanh thu hoạt động', 'Doanh thu từ hoạt động kinh doanh', 'Tổng lợi nhuận gộp', 'EBIT', 'Thu nhập trước thuế']
        calc_table = pivot_helper(df_data, needed)
        rev = calc_table['Doanh thu từ hoạt động kinh doanh']

        calc_table['COGS / Doanh thu'] = calc_table['Giá vốn doanh thu hoạt động'] / rev
        calc_table['Chi phí BH / Doanh thu'] = (calc_table['Tổng lợi nhuận gộp'] - calc_table['EBIT']) / rev
        interest_implied = calc_table['EBIT'] - calc_table['Thu nhập trước thuế']
        calc_table['Lãi vay / Doanh thu'] = np.where(interest_implied > 0, interest_implied / rev, np.nan)
        return melt_helper(calc_table, ['COGS / Doanh thu', 'Chi phí BH / Doanh thu', 'Lãi vay / Doanh thu'], 'Nhóm Cơ cấu Chi phí')
    else:
        needed = ['Tổng giá vốn', 'Doanh thu từ hoạt động kinh doanh', 'EBIT', 'Thu nhập trước thuế']
        calc_table = pivot_helper(df_data, needed)

        for col in needed:
            calc_table[f'{col}_TTM'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(4, min_periods=4).sum())

        rev_ttm = calc_table['Doanh thu từ hoạt động kinh doanh_TTM']
        calc_table['Giá vốn / Doanh thu'] = calc_table['Tổng giá vốn_TTM'] / rev_ttm
        implied_interest_ttm = calc_table['EBIT_TTM'] - calc_table['Thu nhập trước thuế_TTM']
        calc_table['Chi phí lãi vay / Doanh thu'] = np.where(implied_interest_ttm > 0, implied_interest_ttm / rev_ttm, np.nan)
        return melt_helper(calc_table, ['Giá vốn / Doanh thu', 'Chi phí lãi vay / Doanh thu'], 'Nhóm Cơ cấu Chi phí')

def calculate_Nhom_Co_Cau_Tai_San(df_data, period_type):
    if period_type == 'A':
        needed = ['Tổng tài sản ngắn hạn', 'Tổng tài sản', 'Tiền và tương đương tiền', 'Tiền và đầu tư ngắn hạn', 'Khoản vay và phải thu ngắn hạn', 'Tổng hàng tồn kho', 'Tài sản ngắn hạn khác', 'Tổng tài sản dài hạn', 'Tài sản cố định ròng']
        calc_table = pivot_helper(df_data, needed)

        ca = calc_table['Tổng tài sản ngắn hạn']
        calc_table['CA / Tổng TS'] = ca / calc_table['Tổng tài sản']
        calc_table['Tiền / CA'] = calc_table['Tiền và tương đương tiền'] / ca
        dt_ngan_han = calc_table['Tiền và đầu tư ngắn hạn'] - calc_table['Tiền và tương đương tiền']
        calc_table['Đầu tư TC ngắn hạn / CA'] = np.where(dt_ngan_han >= 0, dt_ngan_han / ca, np.nan)
        calc_table['Phải thu NH / CA'] = calc_table['Khoản vay và phải thu ngắn hạn'] / ca
        calc_table['HTK / CA'] = calc_table['Tổng hàng tồn kho'] / ca
        calc_table['TS NH khác / CA'] = calc_table['Tài sản ngắn hạn khác'] / ca
        non_ca_best = calc_table['Tổng tài sản dài hạn'].fillna(calc_table['Tổng tài sản'] - ca)
        calc_table['TS dài hạn / Tổng TS'] = non_ca_best / calc_table['Tổng tài sản']
        calc_table['TSCĐ / Tổng TS'] = calc_table['Tài sản cố định ròng'] / calc_table['Tổng tài sản']
        return melt_helper(calc_table, ['CA / Tổng TS', 'Tiền / CA', 'Đầu tư TC ngắn hạn / CA', 'Phải thu NH / CA', 'HTK / CA', 'TS NH khác / CA', 'TS dài hạn / Tổng TS', 'TSCĐ / Tổng TS'], 'Nhóm Cơ cấu Tài sản')
    else:
        needed = ['Tổng tài sản ngắn hạn', 'Tiền và tương đương tiền', 'Đầu tư ngắn hạn', 'Tổng khoản vay và phải thu', 'Tổng hàng tồn kho', 'Tài sản ngắn hạn khác', 'Tổng Non-ngắn hạn tài sản', 'Tổng tài sản', 'Tài sản cố định ròng']
        calc_table = pivot_helper(df_data, needed)

        ca = calc_table['Tổng tài sản ngắn hạn']
        calc_table['TSNH / Tổng tài sản'] = ca / calc_table['Tổng tài sản']
        calc_table['Tiền / TSNH'] = calc_table['Tiền và tương đương tiền'] / ca
        calc_table['Đầu tư tài chính ngắn hạn / TSNH'] = calc_table['Đầu tư ngắn hạn'] / ca
        calc_table['Phải thu ngắn hạn / TSNH'] = calc_table['Tổng khoản vay và phải thu'] / ca
        calc_table['HTK / TSNH'] = calc_table['Tổng hàng tồn kho'] / ca
        calc_table['TSNH khác / TSNH'] = calc_table['Tài sản ngắn hạn khác'] / ca
        calc_table['TSDH / Tổng tài sản'] = calc_table['Tổng Non-ngắn hạn tài sản'] / calc_table['Tổng tài sản']
        calc_table['TSCĐ / Tổng tài sản'] = calc_table['Tài sản cố định ròng'] / calc_table['Tổng tài sản']
        return melt_helper(calc_table, ['TSNH / Tổng tài sản', 'Tiền / TSNH', 'Đầu tư tài chính ngắn hạn / TSNH', 'Phải thu ngắn hạn / TSNH', 'HTK / TSNH', 'TSNH khác / TSNH', 'TSDH / Tổng tài sản', 'TSCĐ / Tổng tài sản'], 'Nhóm Cơ cấu Tài sản')

def calculate_Nhom_Ngan_Hang(df_data, period_type):
    BANK_TICKERS = ['ARION.IC', 'ISB.IC', 'KVIKA.IC']
    needed = ['Tổng khoản vay và phải thu', 'Tổng nợ phải trả', 'Tổng nợ', 'Tổng tài sản', 'Tổng đầu tư', 'Tiền và đầu tư ngắn hạn', 'Tổng chi phí hoạt động', 'Doanh thu từ hoạt động kinh doanh', 'Vốn cổ đông thường']
    calc_table = pivot_helper(df_data, needed)
    calc_table = calc_table[calc_table['Mã CK'].isin(BANK_TICKERS)].reset_index(drop=True)

    if calc_table.empty: return pd.DataFrame()

    if period_type == 'Q':
        for col in ['Tổng chi phí hoạt động', 'Doanh thu từ hoạt động kinh doanh']:
            calc_table[f'{col}_Calc'] = calc_table.groupby('Mã CK')[col].transform(lambda x: x.rolling(4, min_periods=4).sum())
    else:
        for col in ['Tổng chi phí hoạt động', 'Doanh thu từ hoạt động kinh doanh']:
            calc_table[f'{col}_Calc'] = calc_table[col]

    calc_table['Dư nợ cho vay'] = calc_table['Tổng khoản vay và phải thu']
    calc_table['Nguồn vốn huy động (proxy)'] = calc_table['Tổng nợ phải trả'] - calc_table['Tổng nợ'].fillna(0)
    calc_table['Dư nợ cho vay / Tổng vốn huy động (LDR)'] = calc_table['Dư nợ cho vay'] / calc_table['Nguồn vốn huy động (proxy)']
    calc_table['Dư nợ cho vay / Tổng tài sản'] = calc_table['Dư nợ cho vay'] / calc_table['Tổng tài sản']
    
    earning_assets = calc_table['Tổng khoản vay và phải thu'].fillna(0) + calc_table['Tổng đầu tư'].fillna(0) + calc_table['Tiền và đầu tư ngắn hạn'].fillna(0)
    calc_table['Tài sản Có sinh lãi / Tổng tài sản Có (proxy)'] = earning_assets.replace(0, np.nan) / calc_table['Tổng tài sản']
    calc_table['Tỷ lệ chi phí hoạt động / Tổng thu nhập HĐKD (CIR)'] = calc_table['Tổng chi phí hoạt động_Calc'] / calc_table['Doanh thu từ hoạt động kinh doanh_Calc']
    calc_table['Vốn chủ sở hữu / Tổng vốn huy động'] = calc_table['Vốn cổ đông thường'] / calc_table['Nguồn vốn huy động (proxy)']
    calc_table['Vốn chủ sở hữu / Tổng tài sản Có'] = calc_table['Vốn cổ đông thường'] / calc_table['Tổng tài sản']

    shift_val = 1 if period_type == 'A' else 4
    def compute_growth(col, new_col):
        prev = calc_table.groupby('Mã CK')[col].shift(shift_val)
        calc_table[new_col] = ((calc_table[col] - prev) / prev.abs()).where(prev > 0 if period_type == 'A' else prev != 0, np.nan)

    compute_growth('Dư nợ cho vay', 'Tăng trưởng dư nợ cho vay')
    compute_growth('Nguồn vốn huy động (proxy)', 'Tăng trưởng huy động vốn khách hàng')
    compute_growth('Doanh thu từ hoạt động kinh doanh_Calc' if period_type == 'Q' else 'Doanh thu từ hoạt động kinh doanh', 'Tăng trưởng tổng thu nhập HĐKD trước dự phòng')
    compute_growth('Tổng chi phí hoạt động_Calc' if period_type == 'Q' else 'Tổng chi phí hoạt động', 'Tăng trưởng tổng chi phí HĐKD')

    metrics = ['Dư nợ cho vay', 'Tăng trưởng dư nợ cho vay', 'Nguồn vốn huy động (proxy)', 'Dư nợ cho vay / Tổng vốn huy động (LDR)', 'Dư nợ cho vay / Tổng tài sản', 'Tài sản Có sinh lãi / Tổng tài sản Có (proxy)', 'Tỷ lệ chi phí hoạt động / Tổng thu nhập HĐKD (CIR)', 'Vốn chủ sở hữu / Tổng vốn huy động', 'Vốn chủ sở hữu / Tổng tài sản Có', 'Tăng trưởng huy động vốn khách hàng', 'Tăng trưởng tổng thu nhập HĐKD trước dự phòng', 'Tăng trưởng tổng chi phí HĐKD']
    return melt_helper(calc_table, metrics, 'Nhóm Chỉ số Ngân hàng')

# ==============================================================
# PHẦN 4: PIPELINE TỔNG ĐỂ EXTRACT VÀ TÍNH TOÁN
# ==============================================================
def run_pipeline(filepath: str, period_type: str = 'A'):
    """
    Hàm xử lý hợp nhất (Unified ETL Pipeline)
    - filepath: Đường dẫn file Excel gốc.
    - period_type: 'A' (Annual/Năm) hoặc 'Q' (Quarter/Quý).
    """
    print("=" * 80)
    print(f"[PIPELINE - {period_type}] Bước 1: Extract & Clean dữ liệu từ {filepath}...")
    xls = pd.ExcelFile(filepath)
    
    df_bs_long = to_long(load_and_merge(xls, "BS"), "Cân đối kế toán (BS)", True)
    df_is_long = to_long(load_and_merge(xls, "IS"), "Kết quả kinh doanh (IS)", True)
    df_cf_long = to_long(load_and_merge(xls, "CF"), "Lưu chuyển tiền tệ (CF)", True)
    df_all_data = pd.concat([df_bs_long, df_is_long, df_cf_long], ignore_index=True)

    df_comp_raw = parse_sheet_raw(xls, "COMP", has_date=False)
    if not df_comp_raw.empty:
        df_comp_raw = df_comp_raw.rename(columns={c: vi_translate_indicator(c) for c in df_comp_raw.columns if c != "Ticker"}).rename(columns={"Ticker": "Mã CK"})
        df_comp_long = to_long(df_comp_raw, "Thông tin chung (COMP)", False)
    else:
        df_comp_long = pd.DataFrame()

    df_price_raw = parse_sheet_raw(xls, "PRICE", has_date=True)
    if not df_price_raw.empty and not df_all_data.empty:
        target_dates = df_all_data[['Mã CK', 'Ngày']].drop_duplicates().copy().rename(columns={'Mã CK': 'Ticker', 'Ngày': 'Date'}).sort_values('Date').dropna(subset=['Date'])
        df_price_raw = df_price_raw.sort_values('Date').dropna(subset=['Date'])
        df_price_matched = pd.merge_asof(target_dates, df_price_raw, on='Date', by='Ticker', direction='backward')
        df_price_matched = df_price_matched.rename(columns={c: vi_translate_indicator(c) for c in df_price_matched.columns if c not in ["Ticker", "Date"]}).rename(columns={"Ticker": "Mã CK", "Date": "Ngày"})
        df_price_long = to_long(df_price_matched, "Thị trường (PRICE)", True)
    else:
        df_price_long = pd.DataFrame()

    if 'Ngày' in df_all_data.columns: df_all_data['Ngày'] = pd.to_datetime(df_all_data['Ngày']).dt.strftime('%d/%m/%Y')
    if not df_price_long.empty and 'Ngày' in df_price_long.columns: df_price_long['Ngày'] = pd.to_datetime(df_price_long['Ngày']).dt.strftime('%d/%m/%Y')

    print(f"[PIPELINE - {period_type}] Bước 2 & 3: Tính toán chỉ số chuyên sâu...")
    sheets_to_save = {
        'Tong_Quan_CSTC': calculate_Tong_Quan_CSTC(df_comp_long, df_price_long, df_all_data),
        'Nhom_CSTC_Chung': calculate_Nhom_CSTC_Chung(df_price_long, df_all_data),
        'Nhom_Dinh_Gia': calculate_Nhom_Dinh_Gia(df_price_long, df_all_data, period_type),
        'Nhom_Sinh_Loi': calculate_Nhom_Sinh_Loi(df_all_data, period_type),
        'Nhom_Tang_Truong': calculate_Nhom_Tang_Truong(df_all_data, period_type),
        'Nhom_Thanh_Khoan': calculate_Nhom_Thanh_Khoan(df_all_data, period_type),
        'Nhom_Hieu_Qua_HD': calculate_Nhom_Hieu_Qua_Hoat_Dong(df_all_data, period_type),
        'Nhom_Don_Bay_TC': calculate_Nhom_Don_Bay_Tai_Chinh(df_all_data),
        'Nhom_Dong_Tien': calculate_Nhom_Dong_Tien(df_all_data, period_type),
        'Nhom_Co_Cau_Chi_Phi': calculate_Nhom_Co_Cau_Chi_Phi(df_all_data, period_type),
        'Nhom_Co_Cau_Tai_San': calculate_Nhom_Co_Cau_Tai_San(df_all_data, period_type),
        'Nhom_Ngan_Hang': calculate_Nhom_Ngan_Hang(df_all_data, period_type),
        'BCTC_Can_Doi': filter_data_by_indicators(df_all_data, CHI_SO_CAN_DOI_KE_TOAN, 'BCTC - Cân đối kế toán'),
        'BCTC_KQ_Kinh_Doanh': filter_data_by_indicators(df_all_data, CHI_SO_KET_QUA_KINH_DOANH, 'BCTC - Kết quả kinh doanh'),
        'BCTC_Luu_Chuyen_Tien': filter_data_by_indicators(df_all_data, CHI_SO_LUU_CHUYEN_TIEN_TE, 'BCTC - Lưu chuyển tiền tệ')
    }

    print(f"[PIPELINE - {period_type}] ✅ Hoàn tất đẩy dữ liệu tính toán lên RAM.")
    return df_comp_long, sheets_to_save

# ==============================================================
# PHẦN 5: CHỨC NĂNG XUẤT TEST (WHITE-BOX TESTING)
# ==============================================================
def export_to_excel_for_testing(df_comp, data_dict, output_path):
    """
    Hàm hỗ trợ: Xuất RAM data ra Excel để người dùng Audit (Kiểm thử logic).
    """
    print(f"[*] Đang xuất dữ liệu test ra file: {output_path}")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        if not df_comp.empty:
            df_comp.to_excel(writer, sheet_name='COMP', index=False)
            
        for sheet_name, df_result in data_dict.items():
            if not df_result.empty:
                # Tên sheet tối đa 31 ký tự theo chuẩn Excel
                safe_sheet_name = sheet_name[:31] 
                df_result.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                
    print(f"[+] ✅ File test đã sẵn sàng tại: {output_path}")


if __name__ == "__main__":
    print("=" * 80)
    print("🚀 BẮT ĐẦU CHẠY CHẾ ĐỘ KIỂM THỬ (TEST MODE)")
    print("=" * 80)
    
    # Định nghĩa File Path gốc (Tiến sĩ có thể chỉnh sửa đường dẫn tùy môi trường máy tính)
    FILE_YEAR_INPUT = r"Resource\iceland YEAR.xlsx"
    FILE_QUA_INPUT = r"Resource\iceland QUA.xlsx"
    
    # ----- TEST LUỒNG NĂM -----
    if os.path.exists(FILE_YEAR_INPUT):
        df_comp_y, dict_y = run_pipeline(FILE_YEAR_INPUT, period_type='A')
        export_to_excel_for_testing(df_comp_y, dict_y, r"Output\Test_Bộ_Chỉ_Số_Năm.xlsx")
    else:
        print(f"[-] Không tìm thấy file {FILE_YEAR_INPUT}. Bỏ qua test luồng Năm.")

    print("\n" + "-"*80 + "\n")

    # ----- TEST LUỒNG QUÝ -----
    if os.path.exists(FILE_QUA_INPUT):
        df_comp_q, dict_q = run_pipeline(FILE_QUA_INPUT, period_type='Q')
        export_to_excel_for_testing(df_comp_q, dict_q, r"Output\Test_Bộ_Chỉ_Số_Quý.xlsx")
    else:
        print(f"[-] Không tìm thấy file {FILE_QUA_INPUT}. Bỏ qua test luồng Quý.")
        
    print("=" * 80)
    print("Hoàn tất chế độ Test!")