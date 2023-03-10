import pyspark.pandas as ps
import pandas as pd
import numpy as np
from datetime import date

from bitz.config.core import config
from bitz.validators import TransactionsValidator


class RFVClassificator:
    
    
    def __init__(self, accounts):
        
        self.accounts_full = accounts
        
        self.group_order = config.RFVClassificator_config.group_order
        self.mapper = ps.DataFrame(config.RFVClassificator_config.mapper.items(), columns=['rfvrfv_concat', 'group'])
        self._fit = False
        
        
    def fit(self, X, y = None):
        
        self.transactions = TransactionsValidator(transactions=X).transactions
        self.transactions['value'] = self.transactions['value'].abs()
        
        self.stacked = self.accounts_full[['aid']][~self.accounts_full['status']]
        self.accounts = self.accounts_full[['aid','adate']][self.accounts_full['status']]
        
        # 'Hybernated' - active accounts that have no transactions
        self.hibernated = ps.concat([self.accounts[['aid']],self.transactions[['aid']]]).drop_duplicates(keep=False) 
        
        # 'bonusHunters' - accounts that withdrew bonus only
        self.bonusHunters = self.transactions.drop_duplicates(['aid'], keep=False)
        self.bonusHunters = self.bonusHunters[(self.bonusHunters['value'] <= 20) & self.bonusHunters['ttype'].isin(['pixOut','tedOut'])]
        
        self.rfv_data = ps.concat([self.transactions,self.bonusHunters])\
        .drop_duplicates(keep=False)\
        .groupby('aid')\
        .agg(max_tdate=('tdate','max'), frequency=('aid','count'), sum_value=('value','sum'))\
        .reset_index(level='aid')\
        .merge(self.accounts, on='aid', how='left')\
        .dropna()
        self.rfv_data['recency'] = (ps.to_datetime("today") - self.rfv_data['max_tdate']) // 86400 # calculate recency of all transactions
        self.rfv_data['mfrequency'] = self.rfv_data['frequency'] / ((ps.to_datetime("today") - self.rfv_data['adate']) // 86400)
        self.rfv_data['mvalue'] = self.rfv_data['sum_value'] / ((ps.to_datetime("today") - self.rfv_data['adate']) // 86400)
        self.rfv_data = self.rfv_data.drop(['max_tdate','frequency','sum_value','adate'], axis=1).to_pandas()
        
        self._fit = True
        
        return None
        
        
    def transform(self, X=None):
        
        if self._fit:
            self.quantiles={}

            (self.rfv_data['r'], self.quantiles['r']) = pd.qcut(self.rfv_data['recency'], labels=[4,3,2,1], q=[0,.2,.5,.75,1], retbins=True)
            (self.rfv_data['f'], self.quantiles['f']) = pd.qcut(self.rfv_data['mfrequency'], labels=[1,2,3,4], q=[0,.25,.5,.8,1], retbins=True)
            (self.rfv_data['v'], self.quantiles['v']) = pd.qcut(self.rfv_data['mvalue'], labels=[1,2,3,4], q=[0,.25,.5,.8,1], retbins=True)

            
        
            self.rfv_data[['aid','r','f','v']] = self.rfv_data[['aid','r','f','v']].astype(int)
            self.rfv_data['rfv'] = self.rfv_data['r'] + self.rfv_data['f'] + self.rfv_data['v']
            
            
            self.rfv_data = ps.from_pandas(self.rfv_data)\
                .append(self.stacked.assign(recency=np.nan,mfrequency=np.nan,mvalue=np.nan,r=0,f=0,v=0,rfv=0))\
                .append(self.hibernated.assign(recency=np.nan,mfrequency=np.nan,mvalue=np.nan,r=0,f=0,v=0,rfv=1))\
                .append(self.bonusHunters[['aid']].assign(recency=np.nan,mfrequency=np.nan,mvalue=np.nan,r=0,f=0,v=0,rfv=2))
            
            self.rfv_data['rfvrfv_concat'] = self.rfv_data['r'].astype(str) + self.rfv_data['f'].astype(str) + self.rfv_data['v'].astype(str) + self.rfv_data['rfv'].astype(str)
            
            self.rfv_data = self.rfv_data.merge(self.mapper, on='rfvrfv_concat', how='left')\
                .drop('rfvrfv_concat', axis=1).sort_values('aid')
            
            ##--------------------------------------------------

            #rfv_group_average = self.rfv_data\
            #    .groupby('group')\
            #    .agg(avg_recency=('recency','mean'),avg_mfrequency=('mfrequency','mean'),avg_mvalue=('mvalue','mean'))\
            #    .to_pandas()\
            #    .loc[self.group_order]
    
            self.rfv_group_summary = self.transactions\
                .merge(self.rfv_data[['aid','group']], on='aid', how='left')
    
            self.rfv_group_summary = self.rfv_group_summary\
                .groupby('group')\
                .agg(customers=('aid','nunique'),
                     TPN=('group','count'),
                     TPV=('value','sum'))
            self.rfv_group_summary['ATP'] = self.rfv_group_summary['TPV'] / self.rfv_group_summary['TPN']
            self.rfv_group_summary['TPNperCustomer'] = self.rfv_group_summary['TPN'] / self.rfv_group_summary['customers']
            self.rfv_group_summary['TPVperCustomer'] = self.rfv_group_summary['TPV'] / self.rfv_group_summary['customers']
    
                                                                    
            self.rfv_group_summary = self.rfv_group_summary.join(self.transactions[~self.transactions['ttype'].isin(['pixOut','tedOut'])]\
                                                                     .merge(self.rfv_data[['aid','group']], on='aid', how='left')\
                                                                     .groupby('group')\
                                                                     .agg(notPixFrac=('aid','count')))
            self.rfv_group_summary = self.rfv_group_summary.to_pandas().join(self.transactions.merge(self.rfv_data[['aid','group']], on='aid', how='left')\
                                                                     .groupby(['group','ttype'])\
                                                                     .agg({'value':'sum'})\
                                                                     .reset_index(level=(0,1))\
                                                                     .pivot_table(values='value', index=['group'], columns='ttype')\
                                                                     .to_pandas()\
                                                                     .div(self.rfv_group_summary['TPV'].to_numpy(), axis=0)\
                                                                     .rename(columns=lambda x: x+'TPVshare'))\
                                                  .fillna(0)\
                                                  .loc[self.group_order]
    
            self.rfv_group_summary['notPixFrac'] = self.rfv_group_summary['notPixFrac'] / self.rfv_group_summary['TPN']
    
            self.rfv_group_summary = self.rfv_group_summary.join(self.rfv_data\
                                                                     .groupby('group')\
                                                                     .agg(avg_recency=('recency','mean'),avg_mfrequency=('mfrequency','mean'),avg_mvalue=('mvalue','mean'))\
                                                                     .to_pandas()\
                                                                     .loc[self.group_order],
                                                                 how='left')
    
            self.rfv_group_summary = self.rfv_group_summary.reset_index('group')
        
            ##----------------------------
            
            return self.rfv_data[['aid','r','f','v','rfv','group']], self.rfv_group_summary
        
    def persist(self):

        self.rfv_data[["aid", "r", "f", "v", "rfv", "group"]].to_table("rfv.rfv")
        self.rfv_data[["aid", "r", "f", "v", "rfv", "group"]].to_table(
            "rfv.rfv_" + str(date.today()).replace("-", "_")
        )
        self.rfv_data[["aid", "r", "f", "v", "rfv", "group"]].to_table("rfv.rfv_data")
        self.rfv_data[["aid", "r", "f", "v", "rfv", "group"]].to_table(
            "rfv.rfv_data_" + str(date.today()).replace("-", "_")
        )
        ps.from_pandas(self.rfv_group_summary).to_table("rfv.rfv_group_summary")
        ps.from_pandas(self.rfv_group_summary).to_table(
            "rfv.rfv_group_summary_" + str(date.today()).replace("-", "_")
        )
    
        print("RFV results saved sucessfully!")