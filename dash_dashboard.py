import argparse
from click import parser
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import json
from datetime import datetime, timedelta
import requests
# from mysql.connector import connection
import MySQLdb
# defining style color


colors = {"background": "#000000", "text": "#ffFFFF"}

config = json.load(open("config.json", "r"))
POLYGON_API_KEY = config['polygon_api_key']
FINNHUB_API_KEY = config['finnhub_api_key']


put_color = config['put_color']
call_color = config['call_color']
total_color = config['total_color']
graph_background_color = config['graph_background_color']
graph_line_color = config['graph_line_color']
graph_text_color = config['graph_text_color']
graph_grid_color = config['graph_grid_color']
graph_tick_color = config['graph_tick_color']
line_graph_width = config['line_graph_width']
page_color = config['page_color']

username = config['username']
password = config['password']
database = config['database']
host = config['host']


ticker_list = json.load(open("config.json", "r"))['tickers']

INDICES = config['tickers_which_are_indices']
external_stylesheets = [dbc.themes.SLATE]

MAX_DATA_POINTS = 10000


def get_ini_fig():
    ini_fig = go.Figure(
        data=[
            go.Scatter(
                x=[], y=[], name="total", line_color=total_color, connectgaps=True, line_width=line_graph_width
            ),
            go.Scatter(
                x=[], y=[], name="put", line_color=put_color, connectgaps=True, line_width=line_graph_width
            ),
            go.Scatter(
                x=[], y=[], name="call", line_color=call_color, connectgaps=True, line_width=line_graph_width
            )
        ],
        layout={
            "plot_bgcolor": graph_background_color,
            "paper_bgcolor": graph_background_color,
            "showlegend": True,

        },
    )
    ini_fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(

                color=graph_text_color
            )
        ),
        margin=dict(
            l=0,
            r=0,
            b=0,
            t=0,
            pad=0
        ),
    )

    ini_fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor=graph_line_color,
        gridcolor=graph_grid_color,
        tickfont=dict(color=graph_tick_color),
    )
    ini_fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor=graph_line_color,
        gridcolor=graph_grid_color,
        tickfont=dict(color=graph_tick_color),
    )

    return ini_fig


LAST_CALL_INDEX = {}

LAST_PUT_VALUE = {}

LAST_CALL_VALUE = {}

LAST_TOTAL_VALUE = {}

CURRENT_DATA_POINTS = {}

CURRENT_GRAPH = {}

LAST_TIME_PLOTTED = {}

DO_UPDATE_GRAPHS = {}

EXPIRE_DATE_DELTA = 0
EXPIRE_DATE = (datetime.now()+timedelta(days=EXPIRE_DATE_DELTA)
               ).strftime(r'%Y-%m-%d')


def ini_variables():
    global LAST_CALL_INDEX, LAST_PUT_VALUE, LAST_CALL_VALUE, LAST_TOTAL_VALUE, CURRENT_DATA_POINTS, CURRENT_GRAPH, LAST_TIME_PLOTTED, DO_UPDATE_GRAPHS
    for tick in ticker_list:
        LAST_CALL_INDEX[tick] = 0
        LAST_PUT_VALUE[tick] = 0
        LAST_CALL_VALUE[tick] = 0
        LAST_TOTAL_VALUE[tick] = 0
        CURRENT_DATA_POINTS[tick] = {"total": [],
                                     "call": [],
                                     "put": [],
                                     "time": []

                                     }
        LAST_TIME_PLOTTED[tick] = datetime.now()
        DO_UPDATE_GRAPHS[tick] = True
        CURRENT_GRAPH[tick] = get_ini_fig()


# adding css
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server
app.layout = html.Div(
    style={"backgroundColor": page_color},
    children=[
        html.Div(
            [  # header Div
                dbc.Row(
                    [
                        dbc.Col(
                            html.Header(
                                [
                                    html.H1(
                                        "Stock Dashboard",
                                        style={
                                            "textAlign": "center",
                                            "color": graph_text_color,
                                        },
                                    )
                                ]
                            )
                        )
                    ]
                )
            ]
        ),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Div(
            [  # Dropdown Div
                dbc.Row(
                    [
                        dbc.Col(  # Tickers
                            dcc.Dropdown(
                                id="stock_name",
                                options=[
                                    {
                                        "label": str(ticker_list[i]),
                                        "value": str(ticker_list[i]),
                                    }
                                    for i in range(len(ticker_list))
                                ],
                                searchable=True,
                                value="SPY",
                                placeholder="enter stock name",
                            ),
                            width={"size": 3, "offset": 3},
                        ),
                        dbc.Col(
                            dcc.Slider(min=0, max=5, step=1, value=0,
                                       id='expire_date', verticalHeight=2),


                        )

                    ]
                )
            ]
        ),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            lg=6,
                            children=html.Div([dcc.Interval(
                                id='my_interval',
                                n_intervals=0,       # number of times the interval was activated
                                interval=5*1000,   # update every 2 minutes
                            ),
                                html.Div(id='total-points', children=[
                                    html.H4("Total Points: 0", style={"color": graph_text_color}
                                            )
                                ]),
                                dcc.Graph(
                                id="live price",

                                config={
                                   "displaylogo": False,
                                   "modeBarButtonsToRemove": ["pan2d", "lasso2d"],
                                },
                            )])
                        ),
                        dbc.Col(
                            lg=6,

                            children=html.Div([
                                html.Div(id='current-price', children=[
                                    html.H4("Current Price: 0", style={
                                            "color": graph_text_color})
                                ]),
                                dcc.Interval(
                                    id='my_interval_2',
                                    n_intervals=0,       # number of times the interval was activated
                                    interval=7*1000,   # update every 2 minutes
                                ),
                                dcc.Graph(
                                    style={'height': 400,
                                           'padding': 0, 'margin': 0},
                                    id="bar graph",
                                    config={
                                        "displaylogo": False,
                                        "modeBarButtonsToRemove": ["pan2d", "lasso2d"],
                                    },
                                )])
                        )
                    ]
                ),
            ]
        ),
    ],
)


def modify_data(data, last_time, last_put, last_call, last_total):
    result = []
    # if last_row == 0:
    #     put = 0
    #     call = 0
    #     total = 0
    # else:
    do_update = False
    put = last_put
    call = last_call
    total = last_total

    time_ = last_time  # time, type, sum(val), sum(ask), sum(bid), sum(price)
    for time, type, val, ask, bid, price in data:

        val = abs(val)
        if type == "P":
            if price <= bid:
                put = put+val
            elif price >= ask:
                put = put-val
        elif type == "C":
            if price <= bid:
                call = call-val
            elif price >= ask:
                call = call+val

        put = round(put, 2)
        call = round(call, 2)
        total = round(put+call, 2)
        result.append([total, put, call, time])

        time_ = time
    if time_ != last_time:
        do_update = True

    return result, time_, put, call, total, do_update


# Callback main graph
def get_tick_data(ticker):

    conn = MySQLdb.connect(user=username,
                           host=host,
                           db=database, passwd=password)
    cursor = conn.cursor()

    global LAST_CALL_INDEX, LAST_PUT_VALUE, LAST_CALL_VALUE, LAST_TOTAL_VALUE, CURRENT_DATA_POINTS, EXPIRE_DATE, LAST_TIME_PLOTTED, DO_UPDATE_GRAPHS

    # last_row = LAST_CALL_INDEX[ticker]
    # expire_date =
    last_put = LAST_PUT_VALUE[ticker]

    last_call = LAST_CALL_VALUE[ticker]
    last_total = LAST_TOTAL_VALUE[ticker]
    last_time = LAST_TIME_PLOTTED[ticker]
    do_update_ = DO_UPDATE_GRAPHS[ticker]

    current_time = datetime.now()

    if last_time <= current_time:
        #                                                                                                                                                  and time >'{last_time}'

        query = f"select time, type, sum(val), sum(ask), sum(bid), sum(price) from trading.trades_info where tick='{ticker}' and expire<='{EXPIRE_DATE}' and time >'{last_time}'  and time < ='{current_time.strftime(r'%Y-%m-%d %H:%M:%S')}'   group by time,tick,type  order by time asc;"
        cursor.execute(query)
        data = cursor.fetchall()

        conn.close()

        modified_data, last_time, current_put, current_call, current_total, do_update = modify_data(
            data, last_time, last_put, last_call, last_total)

        total = [x[0] for x in modified_data]
        put = [x[1] for x in modified_data]
        call = [x[2] for x in modified_data]
        time = [x[3] for x in modified_data]

        CURRENT_DATA_POINTS[ticker]['total'] = CURRENT_DATA_POINTS[ticker]['total']+total
        CURRENT_DATA_POINTS[ticker]['put'] = CURRENT_DATA_POINTS[ticker]['put']+put
        CURRENT_DATA_POINTS[ticker]['call'] = CURRENT_DATA_POINTS[ticker]['call']+call
        CURRENT_DATA_POINTS[ticker]['time'] = CURRENT_DATA_POINTS[ticker]['time']+time

        if len(CURRENT_DATA_POINTS[ticker]['total']) > MAX_DATA_POINTS:
            CURRENT_DATA_POINTS[ticker]['total'] = CURRENT_DATA_POINTS[ticker]['total'][-MAX_DATA_POINTS:]
            CURRENT_DATA_POINTS[ticker]['put'] = CURRENT_DATA_POINTS[ticker]['put'][-MAX_DATA_POINTS:]
            CURRENT_DATA_POINTS[ticker]['call'] = CURRENT_DATA_POINTS[ticker]['call'][-MAX_DATA_POINTS:]
            CURRENT_DATA_POINTS[ticker]['time'] = CURRENT_DATA_POINTS[ticker]['time'][-MAX_DATA_POINTS:]

        LAST_TIME_PLOTTED[ticker] = last_time
        LAST_PUT_VALUE[ticker] = current_put
        LAST_CALL_VALUE[ticker] = current_call
        LAST_TOTAL_VALUE[ticker] = current_total
        DO_UPDATE_GRAPHS[ticker] = do_update
        return {"data": modified_data, "last_time": last_time, "current_put": current_put, "current_call": current_call, "current_total": current_total}


def closest(lst, K):
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i]-K))]


def modify_data_aggregation(data):
    # result = []
    put = 0
    call = 0
    total = 0

    for idd, tick, option_contract, time, type, val, ask, bid, price, expire, strike_price in data:

        val = abs(val)
        if type == "P":
            if price <= bid:
                put = put-val
            elif price >= ask:
                put = put+val
        elif type == "C":
            if price <= bid:
                call = call-val
            elif price >= ask:
                call = call+val

        # total = put+call

    # result.append([total, put, call, time])
    return put, call


def get_current_price_of_stock(ticker):

    if ticker in INDICES:
        api_endpoint = "https://api.polygon.io/v3/snapshot/indices"
        params = {
            "ticker.any_of": "I:"+ticker,
            "apiKey": POLYGON_API_KEY
        }

    else:
        # api_endpoint = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        api_endpoint = "https://finnhub.io/api/v1/quote"

        # Define the API parameters
        # params = {
        #     "tickers": ticker,
        #     "apiKey": POLYGON_API_KEY
        # }
        params = {
            "symbol": ticker,
            "token": FINNHUB_API_KEY
        }

    # Send GET request to the Polygon API
    response = requests.get(api_endpoint, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response as
        try:
            data = response.json()
            # Extract the current price from the JSON response
            if ticker in INDICES:
                current_price = float(data["results"][0]["value"])
            else:
                # current_price = float(data["tickers"][0]["lastTrade"]["p"])
                current_price = float(data["c"])
            return current_price
        except Exception as e:
            print(e)
            return 0

    else:
        return 0


def segregate_strike_price(ticker):

    conn = MySQLdb.connect(user=username,
                           host=host,
                           db=database, passwd=password)
    cursor = conn.cursor()

    global EXPIRE_DATE

    query = f"select * from trades_info where tick='{ticker}'  and expire<='{EXPIRE_DATE}' order by time asc;"

    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    current_price = get_current_price_of_stock(ticker)

    if len(data) > 0:

        strike_price = sorted(list(set([float(x[-1]) for x in data])))

        closest_strike_price = closest(strike_price, current_price)
        # print(closest_strike_price)
        index_closest_strike_price = strike_price.index(closest_strike_price)

        lower_bound = index_closest_strike_price - \
            7 if index_closest_strike_price-7 >= 0 else 0
        upper_bound = index_closest_strike_price+8 if index_closest_strike_price + \
            8 <= len(strike_price) else len(strike_price)
        plotted_strike_price = strike_price[lower_bound:upper_bound]
        # print(plotted_strike_price)

        output = []
        for sp in plotted_strike_price:
            filtered_data = [x for x in data if x[-1] == sp]
            put, call = modify_data_aggregation(filtered_data)
            output.append([sp, put, call])

        return output, current_price
    else:
        return [], current_price


@app.callback(
    # output
    [Output("live price", "figure", True), Output('total-points', 'children')],
    # input
    [Input(component_id='my_interval', component_property='n_intervals')],
    # state
    [State("stock_name", "value")],
    prevent_initial_call=True
)
def graph_updater(interval, ticker):
    # print("n_clicks: ", n_clicks)

    global LAST_CALL_INDEX
    # print(LAST_CALL_INDEX)

    result = get_tick_data(ticker)

    global CURRENT_GRAPH, CURRENT_DATA_POINTS

    if CURRENT_GRAPH[ticker]:
        CURRENT_GRAPH[ticker].update_traces(
            y=CURRENT_DATA_POINTS[ticker]['total'], x=CURRENT_DATA_POINTS[ticker]['time'], selector=dict(name="total"))
        CURRENT_GRAPH[ticker].update_traces(
            y=CURRENT_DATA_POINTS[ticker]['call'], x=CURRENT_DATA_POINTS[ticker]['time'], selector=dict(name="call"))
        CURRENT_GRAPH[ticker].update_traces(
            y=CURRENT_DATA_POINTS[ticker]['put'], x=CURRENT_DATA_POINTS[ticker]['time'], selector=dict(name="put"))

        return [CURRENT_GRAPH[ticker], html.H3("Total Points: {}".format(len(CURRENT_DATA_POINTS[ticker]['total'])))]


@app.callback(

    [Output("live price", "figure")],
    # state
    [Input("expire_date", "value")],
    [State("stock_name", "value")]
)
def change_expire_date(expire_date, ticker):
    # print("n_clicks: ", n_clicks)

    global EXPIRE_DATE_DELTA, EXPIRE_DATE, CURRENT_GRAPH
    EXPIRE_DATE_DELTA = int(expire_date)
    EXPIRE_DATE = (datetime.now()+timedelta(days=EXPIRE_DATE_DELTA)
                   ).strftime("%Y-%m-%d")
    ini_variables()

    return [CURRENT_GRAPH[ticker]]


@app.callback(
    # output
    [Output("bar graph", "figure", True), Output('current-price', 'children')],
    # input
    [
        Input(component_id='my_interval_2', component_property='n_intervals')],
    # state
    [State("stock_name", "value")],
    prevent_initial_call=True
)
def graph_updater_bar(interval, ticker):

    result, current_price = segregate_strike_price(ticker)

    if len(result) > 0:
        strike_price = [x[0] for x in result]
        call = [x[2] for x in result]
        put = [x[1] for x in result]

        ini_fig = go.Figure(
            data=[
                go.Bar(name='call', x=strike_price,
                       y=call, marker_color=call_color),
                go.Bar(name='put', x=strike_price,
                       y=put, marker_color=put_color)
            ],
            layout={

                "showlegend": True,
                "plot_bgcolor": graph_background_color,
                "paper_bgcolor": graph_background_color,



            },
        )
    else:
        ini_fig = go.Figure(
            data=[
                go.Bar(name='call', x=[], y=[], marker_color=call_color),
                go.Bar(name='put', x=[], y=[], marker_color=put_color)
            ],
            layout={

                "showlegend": True,
                "plot_bgcolor": graph_background_color,
                "paper_bgcolor": graph_background_color,



            },
        )

    ini_fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(

                color=graph_text_color
            )
        ),
        margin=dict(
            l=0,
            r=0,
            b=0,
            t=0,
            pad=0
        ),
    )

    ini_fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor=graph_line_color,
        gridcolor=graph_grid_color,
        # tickmode='linear',
        tickfont=dict(color=graph_tick_color),
    )
    ini_fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor=graph_line_color,
        gridcolor=graph_grid_color,
        tickfont=dict(color=graph_tick_color),
    )

    return [ini_fig, html.H3("Current Price : {}".format(current_price))]


if __name__ == "__main__":
    ini_variables()
    parser = argparse.ArgumentParser(
        prog='Stock Market Options Delta')
    parser.add_argument('-p', '--port', type=int,
                        default=8050, help='Port to run the server on')
    args = parser.parse_args()

    app.run_server(debug=True, port=args.port)
