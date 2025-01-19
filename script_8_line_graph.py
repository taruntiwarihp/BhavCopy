import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from glob import glob

stock_data = glob("stocks_by_name\*.csv")

for stock in stock_data:
    df_1 = pd.read_csv(stock)
    df = df_1.drop_duplicates(subset='TradDt', keep='first')

    df['TradDt'] = pd.to_datetime(df['TradDt'], format='%Y%m%d')
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.1, subplot_titles=('Stock Prices', 'Trading Volume'),
                        row_heights=[0.7, 0.3])

    # Add the stock price line plot
    fig.add_trace(go.Scatter(x=df['TradDt'], y=df['ClsPric'], mode='lines', name='Closing Price'),
                row=1, col=1)

    # Add the volume bar plot
    fig.add_trace(go.Bar(x=df['TradDt'], y=df['TtlTradgVol'], name='Volume'),
                row=2, col=1)

    height_ = 730 # 600
    width_ = 1400 # 800

    fig.update_layout(height=height_, width=width_, title_text='Stock Price and Volume',
                    xaxis_title='Date', yaxis_title='Price',
                    xaxis2_title='Date', yaxis2_title='Volume',
                    showlegend=False)  # Hide legend for subplot independence

    # fig.show()
    # fig.write_html("scatter_plot.html")

    name = stock.split("\\")[-1].replace("csv", "png")
    fig.write_image(f"stocks_line_graph\{name}")