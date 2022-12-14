'''
This file needs access to data/section_of_route_33_manual.csv

Click on chart and information about points and distances will appear in 
console.
'''

import numpy as np
import shapely.geometry as geom
import matplotlib.pyplot as plt
import pandas as pd
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 
    return c * r


df_route = pd.read_csv('data/section_of_route_33_manual.csv')

class NearestPoint(object):
    def __init__(self, line, ax):
        self.line = line
        self.ax = ax
        ax.figure.canvas.mpl_connect('button_press_event', self)

    def __call__(self, event):
        x, y = event.xdata, event.ydata
        point = geom.Point(x, y)
        distance = self.line.distance(point)
        self.draw_segment(point)
        
        print('Distance to line in degrees:', distance)
        print('-'*30)
        
    def draw_segment(self, point):
        point_on_line = line.interpolate(line.project(point))
        print('Point on route', point_on_line)
        self.ax.plot([point.x, point_on_line.x], [point.y, point_on_line.y], 
                     color='black', marker='o', scalex=False, scaley=False)
        print('Distance to line in meters:', haversine(point.x, point.y, point_on_line.x, point_on_line.y)*1000)
        fig.canvas.draw()

if __name__ == '__main__':
    
    coords = df_route.iloc[:,::-1].values
    line = geom.LineString(coords)

    fig, ax = plt.subplots()
    ax.plot(*coords.T)
    plt.title('Route of tram 33')
    ax.axis('equal')
    NearestPoint(line, ax)
    plt.show()