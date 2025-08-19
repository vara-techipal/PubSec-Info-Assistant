// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

import React from "react";
import ReactDOM from "react-dom/client";
import { HashRouter, Routes, Route, Navigate } from "react-router-dom";
import { initializeIcons } from "@fluentui/react";

import "./index.css";

import { Layout } from "./pages/layout/Layout";
import NoPage from "./pages/NoPage";
import Chat from "./pages/chat/Chat";
import Content from "./pages/content/Content";
import Tutor from "./pages/tutor/Tutor";
import { Tda } from "./pages/tda/Tda";
import Login from "./pages/Login";
import Signup from "./pages/Signup";

const authDisabled = import.meta.env.VITE_AUTH_DISABLED === "true";

const getStoredUser = () => {
    const sessionUser = sessionStorage.getItem("user");
    const localUser = localStorage.getItem("user");
    return sessionUser || localUser;
};

const RequireAuth = ({ children }: { children: JSX.Element }) => {
    if (authDisabled || getStoredUser()) {
        return children;
    }
    return <Navigate to="/login" replace />;
};

initializeIcons();

export default function App() {
    const [toggle, setToggle] = React.useState('Work');
    return (
        <HashRouter>
            <Routes>
                <Route path="/login" element={<Login />} />
                <Route path="/signup" element={<Signup />} />
                <Route
                    path="/"
                    element={
                        <RequireAuth>
                            <Layout />
                        </RequireAuth>
                    }
                >
                    <Route index element={<Chat />} />
                    <Route path="content" element={<Content />} />
                    <Route path="*" element={<NoPage />} />
                    <Route path="tutor" element={<Tutor />} />
                    <Route path="tda" element={<Tda folderPath={""} tags={[]} />} />
                </Route>
            </Routes>
        </HashRouter>
    );
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
    <React.StrictMode>
        <App />
    </React.StrictMode>
);