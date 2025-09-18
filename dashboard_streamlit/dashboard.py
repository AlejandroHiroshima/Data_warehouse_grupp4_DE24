import streamlit as st    
from connect_data_warehouse import query_job_listings
from plots import create_bar_chart

def layout():
    st.set_page_config(layout="wide")
    st.title("Yrkesfakta vägledaren")
    choices =  {'Pedagogik':'mart_p', 
                'Säkerhet och bevakning':'mart_sb', 
                'Transport, distribution, lager':'mart_tdl'}
    selected_occupation_field = st.selectbox("Välj yrkeskategori", options = choices.keys(), index= None)
    if selected_occupation_field:
        df_all = query_job_listings(query=f"select * from {choices[selected_occupation_field]}")
        
        cols = st.columns(3)
        with cols[0]:
            st.metric(label="Total antal annonser", value=int(df_all["VACANCIES"].sum()))
            st.write(f"Top 10 flest yrkesroller för {selected_occupation_field}")
            table = choices[selected_occupation_field]
            df_roles = query_job_listings(f"""
                SELECT
                    OCCUPATION AS "Yrkestitel",
                    SUM(VACANCIES)        AS "Annonser"
                FROM {table}
                GROUP BY OCCUPATION
                ORDER BY "Annonser" DESC
                LIMIT 10
            """)
            st.dataframe(df_roles, hide_index=True)
        
        with cols[1]:
                df_top10_employers = query_job_listings(f"""
            SELECT
                SUM(VACANCIES) AS "Annonser",
                employer_name  AS "Arbetsgivare"
            FROM {choices[selected_occupation_field]}
            GROUP BY 2
            ORDER BY 1 DESC
            LIMIT 10
        """)
                fig_top10 = create_bar_chart(
                    df_top10_employers, 
                    x_col="Arbetsgivare", 
                    y_col="Annonser",
                    xaxis_title="Företag",
                    yaxis_title="Antal annonser",
                    title="Top 10 företag med flest annonser")

    #             # Gör axelrubriker feta och centrera titel
    #             fig.update_layout(
    #             xaxis_title="<b>Arbetsgivare</b>",
    #             yaxis_title="<b>Annonser</b>",
    #             title=dict(
    #             text="Top 10 företag med flest annonser",
    #             x=0.5,  # centrera titel
    #             xanchor="center"
    # )
# )
                st.plotly_chart(fig_top10, use_container_width=True)
            
    
        with cols[2]:
            table = choices[selected_occupation_field]
            df_top5_region = query_job_listings(f"""
            SELECT
                workplace_region AS "Region",
                SUM(vacancies)   AS "Annonser"
            FROM {table}
            GROUP BY workplace_region
            ORDER BY "Annonser" DESC
            LIMIT 5
        """)
            fig_top5 = create_bar_chart(
                df_top5_region, 
                x_col="Region", 
                y_col="Annonser",
                xaxis_title= "Regioner",
                yaxis_title= "Antal annonser",
                title="Top 5 regioner med flest annonser"
                )

#             # Gör axelrubriker feta och centrera titel
#             fig.update_layout(
#             xaxis_title="<b>Region</b>",
#             yaxis_title="<b>Antal annonser</b>",
#             title=dict(
#             text="Top 5 regioner med flest annonser",
#             x=0.5,  # centrera titel
#             xanchor="center"
#     )
# )
            st.plotly_chart(fig_top5, use_container_width=True)

        st.markdown("## Hitta jobbannons")   
        
        cols = st.columns(2)
        
        with cols[0]:
            selected_company = st.selectbox("Välj arbetsgvare:", df_all["EMPLOYER_NAME"].unique(), index= None)    
            if selected_company:
                selected_headline = st.selectbox(
                    "Välj en jobbannons:", df_all.query ("EMPLOYER_NAME == @selected_company")["HEADLINE"], index = None)
                if selected_headline:
                    st.markdown(
                        df_all.query(
                            "HEADLINE == @selected_headline and EMPLOYER_NAME == @selected_company")["DESCRIPTION_HTML_FORMATTED"].values[0], unsafe_allow_html=True)
                              
        with cols[1]:
            st.write("")
        
     
if __name__ == "__main__":
    layout()