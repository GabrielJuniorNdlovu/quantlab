// Predetermined offline fixture renderer. Native compilation/runtime UNVERIFIED.
// This indicator is independent of the Python replay and its audit log.
#property version           "0.10"
#property description       "QuantLab DETERMINISTIC_FIXTURE, no forecasting claim"
#property indicator_chart_window
#property indicator_buffers 1
#property indicator_plots   1
#property indicator_label1  "DETERMINISTIC_FIXTURE UP"
#property indicator_type1   DRAW_ARROW
#property indicator_color1  clrGold
#property indicator_width1  2

input string   FixtureSymbol = "QUANTLAB_BOOM_1000_FIXTURE";
input datetime IssuedAtChartTime = D'2026.01.01 00:00:20';
input double   OriginalAnchorPrice = 1000.12;
input datetime DemoCutoffChartTime = D'2026.01.01 00:00:20';

double ArrowBuffer[];

int OnInit()
{
   if(_Period != PERIOD_M1 || _Symbol != FixtureSymbol)
   {
      Print("QuantLab fixture requires the explicitly named offline fixture symbol on M1.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(IssuedAtChartTime <= 0 || DemoCutoffChartTime <= 0 ||
      !MathIsValidNumber(OriginalAnchorPrice) || OriginalAnchorPrice <= 0)
   {
      Print("QuantLab fixture requires valid explicit chart times and a positive anchor.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(!SetIndexBuffer(0, ArrowBuffer, INDICATOR_DATA) ||
      !ArraySetAsSeries(ArrowBuffer, true) ||
      !PlotIndexSetInteger(0, PLOT_ARROW, 241) ||
      !PlotIndexSetInteger(0, PLOT_ARROW_SHIFT, 0) ||
      !PlotIndexSetInteger(0, PLOT_SHIFT, 0) ||
      !PlotIndexSetDouble(0, PLOT_EMPTY_VALUE, EMPTY_VALUE) ||
      !IndicatorSetString(INDICATOR_SHORTNAME, "QuantLab FIXTURE renderer (UNVERIFIED)"))
   {
      PrintFormat("QuantLab fixture initialization failed: %d", GetLastError());
      return INIT_FAILED;
   }
   return INIT_SUCCEEDED;
}

int OnCalculate(const int rates_total,
                const int prev_calculated,
                const datetime &time[],
                const double &open[],
                const double &high[],
                const double &low[],
                const double &close[],
                const long &tick_volume[],
                const long &volume[],
                const int &spread[])
{
   if(!ArraySetAsSeries(time, true))
   {
      PrintFormat("QuantLab fixture time indexing failed: %d", GetLastError());
      return 0;
   }
   // Explicitly initialize every buffer slot; no price OHLC determines the anchor.
   ArrayInitialize(ArrowBuffer, EMPTY_VALUE);
   if(DemoCutoffChartTime < IssuedAtChartTime)
      return rates_total;

   const datetime anchor_minute = (datetime)((long)IssuedAtChartTime / 60 * 60);
   for(int bar = 0; bar < rates_total; bar++)
   {
      // Exact opening-minute match prevents attaching an arrow across a history gap.
      if(time[bar] == anchor_minute)
      {
         ArrowBuffer[bar] = OriginalAnchorPrice;
         return rates_total;
      }
   }
   return rates_total;
}
